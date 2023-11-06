from pathlib import Path

import modal
from modal import Image, Mount, Secret, Stub, asgi_app, gpu, method

import asyncio

GPU_CONFIG = gpu.A100()
#MODEL_ID = "TabbyML/StarCoder-3B"#os.environ.get("MODEL_ID", "TabbyML/StarCoder-3B")
MODEL_ID = "TabbyML/StarCoder-3B"
LAUNCH_FLAGS = ["serve", "--model", MODEL_ID, "--port", "8000", "--device", "cuda"]

from datetime import datetime

def download_model():
    import subprocess

    subprocess.run(
        [
            "/opt/tabby/bin/tabby",
            "download",
            "--model",
            MODEL_ID,
        ]
    )


image = (
    Image.from_registry(
        "tabbyml/tabby:0.5.0",
        add_python="3.11",
    )
    .dockerfile_commands("ENTRYPOINT []")
    .run_function(download_model)
    .pip_install(
        "git+https://github.com/TabbyML/tabby.git#egg=tabby-python-client&subdirectory=experimental/eval/tabby-python-client"
    )
)

stub = Stub("tabby-" + MODEL_ID.split("/")[-1], image=image)


@stub.cls(
    gpu=GPU_CONFIG,
    concurrency_limit=10,
    allow_concurrent_inputs=1,
    container_idle_timeout=60 * 10,
    timeout=360,
)
class Model:
    def __enter__(self):
        import socket
        import subprocess, os
        import time

        from tabby_python_client import Client

        my_env = os.environ.copy()
        my_env["TABBY_DISABLE_USAGE_COLLECTION"] = "1"
        self.launcher = subprocess.Popen(["/opt/tabby/bin/tabby"] + LAUNCH_FLAGS, env=my_env)
        self.client = Client("http://127.0.0.1:8000", timeout=120)

        # self.client.raise_on_unexpected_status = True

        # Poll until webserver at 127.0.0.1:8000 accepts connections before running inputs.
        def webserver_ready():
            try:
                socket.create_connection(("127.0.0.1", 8000), timeout=1).close()
                return True
            except (socket.timeout, ConnectionRefusedError):
                # Check if launcher webserving process has exited.
                # If so, a connection can never be made.
                retcode = self.launcher.poll()
                if retcode is not None:
                    raise RuntimeError(
                        f"launcher exited unexpectedly with code {retcode}"
                    )
                return False

        while not webserver_ready():
            time.sleep(1.0)

        print("Tabby server ready!")

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.launcher.terminate()

    @method()
    async def health(self):
        from tabby_python_client.api.v1 import health

        resp = await health.asyncio(client=self.client)
        return resp.to_dict()

    @method()
    async def complete(self, language, crossfile_context, json_line):
        import traceback 
        from tabby_python_client.api.v1 import completion
        from tabby_python_client.models import (
            CompletionRequest,
            DebugOptions,
            CompletionResponse,
            Segments,
        )
        from tabby_python_client.types import Response
        from tabby_python_client import errors
        import json

        obj = json.loads(json_line)
        if crossfile_context:
            prompt = obj["crossfile_context"]["text"] + obj["prompt"]
        else:
            prompt = obj["prompt"]
        groundtruth = obj["groundtruth"]
        #print(f'prompt: {prompt}')
        #print(f"groundtruth: {groundtruth} begin at {datetime.now()}")
        request = CompletionRequest(
            language=language, debug_options=DebugOptions(raw_prompt=prompt)
        )
        # resp: CompletionResponse = await completion.asyncio(
        #     client=self.client, json_body=request
        # )
        try:
            resp: Response = await completion.asyncio_detailed(
                client=self.client, json_body=request
            )
            #print(resp.status_code)

            #print(f"groundtruth: {groundtruth} end at {datetime.now()}")
            if resp.parsed != None:
                return (prompt, groundtruth, resp.parsed.choices[0].text)
            else:
                return (prompt, groundtruth, f"status code: <{resp.status_code}>")
        except errors.UnexpectedStatus as e:
            #print(e)
            return (prompt, groundtruth, f"error: code={e.status_code} content={e.content} error={e}")
        except Exception as e:
            print(f'error occurs!!! {type(e)}')
            #traceback.print_exc()
            return None, None, None

@stub.local_entrypoint()
async def main(language, file):
    import json

    model = Model()
    print(model.health.remote())

    input_file = "./data/" + MODEL_ID.split("/")[-1] + "/" + language + "/" + file
    output_file = "./data/" + MODEL_ID.split("/")[-1] + "/" + language + "/output_" + file

    if file == 'line_completion.jsonl':
        crossfile_context = False
    else:
        crossfile_context = True
        

    # with open(output_file, "w") as fout:
    #     with open(input_file) as fin:
    #         for line in fin:
    #             x = json.loads(line)
    #             if crossfile_context:
    #                 prompt = x["crossfile_context"]["text"] + x["prompt"]
    #             else:
    #                 prompt = x["prompt"]
    #             label = x["groundtruth"]
                
    #             prediction = model.complete.remote(language, prompt)
                
    #             json.dump(dict(prompt=prompt, label=label, prediction=prediction), fout)
    #             fout.write("\n")
    
    output = []
    with open(input_file) as fin:
        output = await asyncio.gather(*[model.complete.remote.aio(language, crossfile_context, line) for line in fin])
        # for line in fin:
        #     o = model.complete.local(language, crossfile_context, line)
        #     output.append(o)

    with open(output_file, "w") as fout:
        for prompt, label, prediction in output:
            json.dump(dict(prompt=prompt, label=label, prediction=prediction), fout)
            fout.write("\n")

