defmodule Elasticsearch.API.HTTP do
  @moduledoc """
  A "real" HTTP implementation of `Elasticsearch.API`.
  """
  use CarReq

  @behaviour Elasticsearch.API

  @impl true
  def request(config, method, url, data, opts) do
    merged_opts =
      config
      |> Map.get(:default_options, [])
      |> Keyword.delete(:aws)
      |> Keyword.merge(opts)

    [
      base_url: Map.get(config, :url),
      method: method,
      url: url,
      headers: Map.get(config, :default_headers, []) ++ [content_type: "application/json"]
    ]
    |> Keyword.merge(process_request_body(data))
    |> Keyword.merge(auth_credentials(config))
    |> Keyword.merge(merged_opts)
    |> request()
  end

  # Converts the request body into JSON, unless it has already
  # been converted. If the data is empty, sends ""
  defp process_request_body(data) when is_binary(data) do
    [body: data]
  end

  defp process_request_body(data) when is_map(data) and data != %{} do
    [json: data]
  end

  defp process_request_body(_data) do
    [body: ""]
  end

  defp auth_credentials(%{username: username, password: password}) do
    [auth: {username, password}]
  end

  defp auth_credentials(_config) do
    []
  end
end
