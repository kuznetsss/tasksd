local dispatcher = {
    notification = function(method, params)
        vim.print("Got notification: " .. method)
    end,
    server_request = function(method, params)
        vim.print("Got server_request: " .. method)
    end,
    on_exit = function(code, signal)
        vim.print("Got on_exit: " .. code .. ", " .. signal)
    end,
    on_error = function(code, err)
        vim.print("Got error: " .. code)
    end,
}

local client = vim.lsp.rpc.connect("/tmp/tasksd_socket")(dispatcher)

client.request("bbb", {}, function(err, result)
    vim.print("callback")
    if err then
        vim.print("Error: " .. vim.inspect(err))
    else
        vim.print("Result: " .. vim.inspect(result))
    end
end)

client.request("a", {}, function(err, result)
    vim.print("callback")
    if err then
        vim.print("Error: " .. vim.inspect(err))
    else
        vim.print("Result: " .. vim.inspect(result))
    end
end)
