defmodule CachedPaginator.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/TODO/cached_paginator"

  def project do
    [
      app: :cached_paginator,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      description: description(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    ETS-backed pagination cache with cursor support.
    Cache expensive query results once, paginate efficiently with stable cursors.
    """
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
