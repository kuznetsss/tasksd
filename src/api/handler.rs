use std::sync::Arc;

use rustix::process::Signal;

use crate::{
    api::{
        common::RequestId,
        request::{Request, RequestBody, TaskSendSignalParams, TaskStartParams},
        response::{Response, ResponseBody, ResponseResult},
    },
    tasks::task_manager::TaskManager,
};

pub async fn handle_request(request: Request, task_manager: Arc<TaskManager>) -> Response {
    let body = match request.body {
        RequestBody::TaskStart(start_params) => start_task(start_params, task_manager),
        RequestBody::TaskSendSignal(signal_params) => {}
    };
    Response {
        id: request.id,
        body,
    };
}

fn start_task(start_params: TaskStartParams, task_manager: Arc<TaskManager>) -> ResponseBody {
    let task_builder = task_manager.create_task(start_params.executable);
    if let Some(args) = start_params.args {
        task_builder.args(args);
    }
    if let Some(wd) = start_params.working_dir {
        task_builder.working_dir(wd);
    }
    if start_params.subscribe_to_output {
        task_builder.on_output(|_| todo!());
    }
    match task_builder.submit() {
        Ok(task_id) => ResponseBody::Result(ResponseResult::StartTaskResult { task_id }),
        Err(e) => e.into(),
    }
}

fn send_signal(
    send_signal_params: TaskSendSignalParams,
    task_manager: Arc<TaskManager>,
) -> ResponseBody {
    let task = task_manager.get_task(TaskId(send_signal_params.task_id))?;
    let signal = Signal::try_from(send_signal_params.signal).unwrap();
    task.send_signal(send_signal_params.signal)?;
    ResponseBody::Result(ResponseResult::SendSignalResult)
}
