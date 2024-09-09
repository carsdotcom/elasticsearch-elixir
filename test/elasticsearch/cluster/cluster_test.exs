defmodule Elasticsearch.ClusterTest do
  use ExUnit.Case, async: false

  def valid_config do
    %{
      api: Elasticsearch.API.HTTP,
      json_library: Jason,
      url: "http://localhost:9200",
      username: "username",
      password: "password",
      indexes: %{
        posts: %{
          settings: "test/support/settings/posts.json",
          store: Elasticsearch.Test.Store,
          sources: [Post, Comment],
          bulk_page_size: 5000,
          bulk_wait_interval: 5000
        }
      },
      default_options: [
        aws: [
          region: "us-east-1",
          service: "es",
          access_key: "aws_access_key_id",
          secret: "aws_secret_access_key"
        ]
      ]
    }
  end

  setup do
    Application.put_env(
      :elasticsearch,
      Elasticsearch.ClusterTest.MixConfiguredCluster,
      valid_config()
    )
  end

  defmodule Cluster do
    use Elasticsearch.Cluster
  end

  defmodule MixConfiguredCluster do
    use Elasticsearch.Cluster, otp_app: :elasticsearch
  end

  defmodule InitConfiguredCluster do
    use Elasticsearch.Cluster

    def init(_config) do
      {:ok, Elasticsearch.ClusterTest.valid_config()}
    end
  end

  describe "__config__/0" do
    test "accepts Mix configuration" do
      assert {:ok, _pid} = MixConfiguredCluster.start_link()

      assert MixConfiguredCluster.__config__() ==
               Map.put(valid_config(), :http_supervisor_options,
                 name: MixConfiguredCluster.FinchSupervisor
               )
    end

    # this feels like a way around the config building, which is important and which happens in
    # __MODULE__.start_link/1.
    # if you do this in your module, then there's a non-zero amount of configuring that 'you' will
    # be responsible for and which you have to get right.
    test "accepts init configuration" do
      assert {:ok, _pid} = InitConfiguredCluster.start_link()
      # init config bypasses the start_link callback; I guess that's right??
      assert InitConfiguredCluster.__config__() == valid_config()
    end

    test "accepts configuration on startup" do
      assert {:ok, _pid} = Cluster.start_link(valid_config())

      assert Cluster.__config__() ==
               Map.put(valid_config(), :http_supervisor_options, name: Cluster.FinchSupervisor)
    end

    test "saves the Finch name" do
      assert {:ok, _pid} = Cluster.start_link(valid_config())
      saved_config = Cluster.__config__()

      assert Keyword.get(saved_config.http_supervisor_options, :name) == Cluster.FinchSupervisor
    end

    test "set the conn_opts config and receive a connect timeout" do
      config =
        Map.put(valid_config(), :http_supervisor_options,
          name: Cluster.FinchSupervisor,
          pools: %{
            :default => [size: 75]
          },
          conn_opts: [transport_opts: [timeout: 499]]
        )

      assert {:ok, _pid} = Cluster.start_link(config)
      saved_config = Cluster.__config__()

      assert Keyword.get(saved_config.http_supervisor_options, :conn_opts) == [
               transport_opts: [timeout: 499]
             ]

      # a connect timeout (i.e. a tls handshake timeout, raises %Req.TransportError{})
      adapter = fn request ->
        {request, %Req.TransportError{reason: :timeout}}
      end

      {:error, %Req.TransportError{reason: :timeout}} =
        Elasticsearch.get(Cluster, "/_cat/health?format=json", adapter: adapter)
    end
  end

  describe ".start_link/1" do
    test "validates url" do
      refute errors_on(url: "http://localhost:9200")[:url]
      assert errors_on(url: "werlkjweoqwelj").url
    end

    test "validates username" do
      assert {"must be present", validation: :presence} in errors_on(%{password: "password"}).username
      refute errors_on([])[:username]
    end

    test "validates password" do
      assert {"must be present", validation: :presence} in errors_on(%{username: "username"}).password
      refute errors_on([])[:password]
    end

    test "validates api" do
      assert {"must be present", validation: :presence} in errors_on([]).api

      for invalid <- [Nonexistent.Module, "string"] do
        assert {"must be valid", validation: :by} in errors_on(api: invalid).api
      end
    end

    test "validates json_library" do
      refute errors_on([])[:json_library]
      refute errors_on(json_library: Poison)[:json_library]

      assert {"must be valid", validation: :by} in errors_on(json_library: Nonexistent.Module).json_library
    end

    test "validates indexes" do
      errors = errors_on(%{valid_config() | indexes: %{example: %{}}})

      for field <- [:store, :sources, :bulk_page_size, :bulk_wait_interval] do
        assert {"must be present", validation: :presence} in errors[field]
      end

      errors =
        errors_on(%{
          valid_config()
          | indexes: %{example: %{settings: :atom, store: Nonexistent.Module, sources: 123}}
        })

      for field <- [:settings, :store, :sources] do
        assert {"must be valid", validation: :by} in errors[field]
      end
    end

    test "accepts valid configuration" do
      assert {:ok, pid} = Cluster.start_link(valid_config())
      assert is_pid(pid)
    end

    test "starts a Finch supervisor with a default name" do
      assert {:ok, _pid} = Cluster.start_link(valid_config())
      pid = Process.whereis(Cluster.FinchSupervisor)
      assert is_pid(pid)
    end
  end

  describe "start_finch/1" do
    # these teste are really testing Finch internals and that could make them brittle.
    # I still think they're valuable to ensure that we are setting the config that we
    # believe we are. That said if (when) they break, don't spend too much time fixing them.
    test "has default config" do
      assert {:ok, _pid} = Cluster.start_link(valid_config())
      config = Cluster.__config__()
      assert {:ok, pid} = Cluster.start_finch(config)
      state = :sys.get_state(pid)
      config = elem(state, 11)

      supervisor_pid = Process.whereis(Cluster.FinchSupervisor)
      assert is_pid(supervisor_pid)

      assert config
             |> Map.get(:default_pool_config)
             |> Map.get(:size) == 50
    end

    test "uses config to start the Finch" do
      adapter_config = [
        name: Cluster.CustomFinch,
        pools: %{
          "http://localhost:1234/path/gets/ignored?true" => [size: 99, protocols: [:http2], count: 3],
          :default => [size: 300]
        }
      ]

      config = Map.put(valid_config(), :http_supervisor_options, adapter_config)
      assert {:ok, _pid} = Cluster.start_link(config)

      assert {:ok, pid} = Cluster.start_finch(config)
      state = :sys.get_state(pid)
      config = elem(state, 11)

      custom_pid = Process.whereis(Cluster.CustomFinch)
      assert is_pid(custom_pid)

      config
      |> Map.get(:pools)
      # the url is decomposed into a three-tuple: :scheme, :host, :port
      |> Map.get({:http, "localhost", 1234})
      |> then(fn pool_config ->
        assert Map.get(pool_config, :count) == 3
        assert Map.get(pool_config, :size) == 99
        conn_opts = Map.get(pool_config, :conn_opts)
        assert Keyword.get(conn_opts, :protocols) == [:http2]
      end)

      assert config
             |> Map.get(:default_pool_config)
             |> Map.get(:size) == 300
    end
  end

  defp errors_on(config) do
    {:error, errors} = Cluster.start_link(config)
    errors
  end
end
