module Gateway
  class HTTP < Gateway::Base
    # purge connection that is stuck in bad state
    categorize_error(Net::HTTP::Pipeline::PipelineError, {
      :as   => :retry,
      :for  => :pipeline
    }) do | gateway |
      gateway.purge_current_connection!
    end

    categorize_error Net::HTTP::Pipeline::Error,
                     :as => :bad_gateway, :for => :pipeline

    categorize_error Net::HTTPError,
                     :as => :bad_gateway, :for => :all

    # It's safe to specify all actions because non-idempotent requests
    # will skip retry automatically
    categorize_error Timeout::Error, Net::HTTPError, Net::HTTP::Pipeline::ResponseError,
                     :as => :retry, :for => :all

    def self.normalize_uri uri
      return uri if uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)
      uri = uri.to_s
      uri = "http://#{uri}" unless /^(http|https):\/\// =~ uri
      URI.parse(uri)
    end

    attr_reader   :address, :use_ssl, :host, :port
    attr_accessor :header

    def initialize(name, opts)
      super
      @address   = self.class.normalize_uri(opts[:uri])
      @use_ssl  = @address.scheme == "https"
      @host     = @address.host
      @port     = @address.port
      @header   = opts[:header] || {}
    end

    def pipeline(requests, opts={}, &block)
      msg = requests.map{|r| absolute_url(r).to_s }.join(',')

      execute('pipeline', msg, opts) do |conn|
        conn.start unless conn.started?
        conn.pipeline(requests.dup, &block)
      end
    end

    def request(req, opts={})
      opts = {
        :persistent => false,
        :retry      => false
      }.merge(opts) unless idempotent?(req)

      action = req.method.downcase.to_sym

      execute(action, absolute_url(req), opts) do |conn|
        conn.start unless conn.started?
        rsp = conn.request(req)
        validate_response(req, rsp, valid_responses(opts)) if validate_response?(opts)
        rsp
      end
    end

    def absolute_url(req)
      address + req.path
    end

    def head(path, header=nil, opts={})
      req = prepare_request(:head, path, nil, header)
      request(req, opts)
    end

    def get(path, header=nil, opts={})
      req = prepare_request(:get, path, nil, header)
      request(req, opts)
    end

    def post(path, body=nil, header=nil, opts={})
      req = prepare_request(:post, path, body, header)
      request(req, opts)
    end

    def put(path, body=nil, header=nil, opts={})
      req = prepare_request(:put, path, body, header)
      request(req, opts)
    end

    def delete(path, header=nil, opts={})
      req = prepare_request(:delete, path, nil, header)
      request(req, opts)
    end

    def idempotent?(req)
      case req
      when Net::HTTP::Delete, Net::HTTP::Get, Net::HTTP::Head,
           Net::HTTP::Options, Net::HTTP::Put, Net::HTTP::Trace then
        true
      end
    end

    def validate_response?(opts)
      opts.fetch(:validate_response, true)
    end

    def valid_responses(opts)
      opts.fetch(:valid_responses, [ Net::HTTPSuccess ])
    end

    def validate_response(req, rsp, valid_rsp)
      is_valid = valid_rsp.any?{|klass| rsp.is_a?(klass) }

      raise Gateway::BadResponse.new(
        "Invalid Response",
        :status => rsp.code,
        :url => absolute_url(req)
      ) unless is_valid
    end

    def prepare_request(method, path, body, header)
      klass = "Net::HTTP::#{method.to_s.classify}".constantize

      header = self.header.merge(header || {})
      req   = klass.new path, header

      if allow_body?(req)
        if body.is_a?(Hash)
          req.set_form_data body
        elsif body.respond_to?(:rewind) && body.respond_to?(:read)
          body.rewind
          req.body = body.read
        else
          req.body = body.to_s
        end
      end
      req
    end

    def allow_body?(req)
      req.is_a?(Net::HTTP::Post) || req.is_a?(Net::HTTP::Put)
    end

    def read_timeout
      options[:read_timeout]
    end

    def open_timeout
      options[:open_timeout]
    end


    protected

    def success_status(resp)
      resp.code
    end

    def success_message(resp)
      resp.message
    end

    def connect
      conn = Net::HTTP.new(host, port)
      conn.use_ssl = use_ssl
      conn.read_timeout = read_timeout if read_timeout
      conn.open_timeout = open_timeout if open_timeout
      conn
    end

    def disconnect(conn)
      conn.finish
    rescue IOError
    end

    def reconnect(conn)
      disconnect(conn)
      conn
    end
  end
end
