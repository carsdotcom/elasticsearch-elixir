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
      |> Keyword.merge(opts)
      |> Keyword.delete(:aws_sign_from_exaws)

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
    [auth: {:basic, "#{username}:#{password}"}]
  end

  if Code.ensure_loaded?(ExAws) do
    defp auth_credentials(%{default_options: default_options}) do
      if Keyword.get(default_options, :aws_sign_from_exaws) do
        # Build auth from ExAWS and pass to https://hexdocs.pm/req/Req.Steps.html#put_aws_sigv4/1
        config =
          Map.take(ExAws.Config.new(:es), [
            :region,
            :access_key_id,
            :secret_access_key,
            :security_token
          ])

        aws_sigv4 = [
          service: "es",
          region: config[:region],
          access_key_id: config[:access_key_id],
          secret_access_key: config[:secret_access_key]
        ]

        aws_sigv4 =
          if config[:security_token],
            do: aws_sigv4 ++ [token: config[:security_token]],
            else: aws_sigv4

        [
          aws_sigv4: aws_sigv4
        ]
      else
        []
      end
    end
  end

  defp auth_credentials(_config) do
    []
  end
end
