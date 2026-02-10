defmodule RecoverableStreamEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :recoverable_stream_ex,
      version: "1.0.0",
      elixir: "~> #{elixir_version()}",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      description: description(),
      package: package(),
      name: "RecoverableStreamEx",
      source_url: "https://github.com/sumerman/recoverable_stream_ex",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      preferred_cli_env: [
        check: :test
      ]
    ]
  end

  def application do
    [
      mod: {RecoverableStreamEx, []},
      extra_applications: [:logger]
    ]
  end

  defp description do
    """
    By extracting evaluation of the source stream into a separate
    process `RecoverableStream` provides a way to isolate upstream
    errors and recover from them.
    """
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/sumerman/recoverable_stream_ex"}
    ]
  end

  defp deps do
    [
      {:nimble_options, "~> 1.0"},
      {:postgrex, "~> 0.15.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18.5", only: [:test]}
    ]
  end

  defp aliases do
    [
      check: [
        "compile --warnings-as-errors",
        "format --check-formatted",
        "test --warnings-as-errors"
      ]
    ]
  end

  defp elixir_version do
    ".tool-versions"
    |> File.read!()
    |> String.split("\n")
    |> Enum.find(&String.starts_with?(&1, "elixir"))
    |> String.split()
    |> Enum.at(1)
  end
end
