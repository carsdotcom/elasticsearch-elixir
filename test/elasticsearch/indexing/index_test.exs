defmodule Elasticsearch.IndexTest do
  use Elasticsearch.DataCase, async: true

  alias Elasticsearch.{
    Index,
    Namespace,
    Test.Cluster
  }

  # the doctests are OK, but there is variability in the responses (map sorting, etc) that makes them
  # ❄️ esp on CI/GH Actions.
  # doctest Elasticsearch.Index

  defmodule ErrorAPI do
    @behaviour Elasticsearch.API

    @impl true
    def request(_config, :get, _url, _data, _opts) do
      {:ok,
       %{
         status: 200,
         body: [%{"index" => "index-123"}]
       }}
    end

    def request(_config, :delete, _url, _data, _opts) do
      {:ok,
       %{
         status: 404,
         body: "index not found"
       }}
    end
  end

  setup do
    Namespace.set_pid_namespace(self())
    ns = Namespace.get_pid_namespace(self())

    on_exit(fn ->
      Namespace.set_pid_namespace(self())

      Elasticsearch.delete(Cluster, "/#{ns}*")
    end)
  end

  describe ".clean_starting_with/3" do
    test "handles errors" do
      assert {:error, [%Elasticsearch.Exception{message: "index not found"}]} =
               Cluster
               |> Elasticsearch.Cluster.Config.get()
               |> Map.put(:api, ErrorAPI)
               |> Index.clean_starting_with("index", 0)
    end
  end
end
