defmodule Elasticsearch.API.HTTPTest do
  use ExUnit.Case

  alias Elasticsearch.API.HTTP

  describe ".request/5" do
    test "respects absolute URLs" do
      assert {:ok, %{body: body}} =
               HTTP.request(
                 %{},
                 :get,
                 "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9200/_cat/health",
                 "",
                 []
               )

      assert is_binary(body)
    end

    test "handles HTTP errors" do
      assert {:error, %{}} =
               HTTP.request(
                 %{},
                 :get,
                 "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9999/nonexistent",
                 "",
                 []
               )
    end

    test "accepts a Req adapter" do
      adapter = fn request ->
        response = %Req.Response{status: 200, body: "Super Adapter"}
        {request, response}
      end

      {:ok, resp} =
        HTTP.request(
          %{},
          :get,
          "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9200/_cat/health",
          "",
          adapter: adapter
        )

      assert resp.body == "Super Adapter"
    end

    test "accepts Req options" do
      {:error, error} =
        HTTP.request(
          %{},
          :get,
          "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9200/_cat/health",
          "",
          pool_timeout: 0
        )

      assert error == :pool_timeout

      {:error, error} =
        HTTP.request(
          %{},
          :get,
          "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9200/_cat/health",
          "",
          receive_timeout: 0
        )

      assert error == %Mint.TransportError{reason: :timeout}
    end

    # See https://github.com/danielberkompas/elasticsearch-elixir/issues/81
    @tag :regression
    test "handles timeouts" do
      assert {:error, %{reason: :timeout}} =
               HTTP.request(
                 %{},
                 :get,
                 "http://#{System.get_env("ELASTICSEARCH_HOST", "localhost")}:9200/_cat/health",
                 "",
                 receive_timeout: 0
               )
    end
  end
end
