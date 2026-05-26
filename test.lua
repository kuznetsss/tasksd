local dispatcher = {
    notification = function(method, params)
        vim.print("Got notification: " .. method .. " " .. vim.inspect(params))
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

local client = vim.lsp.rpc.connect("/tmp/tasksd_test")(dispatcher)

client.request("task.start", { executable = "ls" }, function(err, result)
    vim.print("callback")
    if err then
        vim.print("Error: " .. vim.inspect(err))
    else
        vim.print("Result: " .. vim.inspect(result))
    end
end)
