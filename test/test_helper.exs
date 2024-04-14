ExUnit.start()
Application.ensure_started(:logger)
# suppress debug logs in test
Logger.configure(level: :error)
