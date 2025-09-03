import os
import cv2
import getpass, socket, platform, hashlib
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.attribute import Attribute, AttributeType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# 🔹 Definir o fluxo de dados
dataflow_tag = "extracaoframes"
dflow = Dataflow(dataflow_tag)

transf = Transformation("ExtrairFrames")

input_set  = Set("VideosEntrada",  SetType.INPUT,
                 [Attribute("video_path", AttributeType.FILE)])
output_set = Set("FramesGerados", SetType.OUTPUT,
                 [Attribute("path", AttributeType.TEXT)])

system_set = Set("system_metadata2", SetType.INPUT, [
    Attribute("executed_by",     AttributeType.TEXT),
    Attribute("cwd",             AttributeType.TEXT),
    Attribute("hostname",        AttributeType.TEXT),
    Attribute("python_version",  AttributeType.TEXT),
])

script_set = Set("script_metadata2", SetType.INPUT, [
    Attribute("start_time",           AttributeType.TEXT),
    Attribute("end_time",             AttributeType.TEXT),
    Attribute("script_path",          AttributeType.TEXT),
    Attribute("script_last_modified", AttributeType.TEXT),
    Attribute("script_hash",          AttributeType.TEXT),
])

transf.set_sets([input_set, output_set, system_set, script_set])
dflow.add_transformation(transf)
dflow.save()

# 🔹 Metadados fixos
executed_by     = getpass.getuser()
cwd             = os.getcwd()
hostname        = socket.gethostname()
python_version  = platform.python_version()

script_path         = os.path.abspath(__file__)
script_last_modified= datetime.fromtimestamp(os.path.getmtime(__file__)).isoformat()
with open(__file__, 'rb') as f:
    script_hash = hashlib.sha256(f.read()).hexdigest()

# 🔹 Garantir diretório de saída
frames_dir = Path("frames").resolve()
frames_dir.mkdir(parents=True, exist_ok=True)

# =========================
# 🔹 Execução real da tarefa
# =========================
count = 1
with os.scandir('videos_salvos') as i:
    for entry in i:
        if not (entry.is_file() and entry.name != ".DS_Store"):
            continue

        print(f"\n🎬 Iniciando extração de frames para: {entry.name}")
        task = Task(count, dataflow_tag, "ExtrairFrames")
        task.begin()

        start_time = datetime.now().isoformat()

        # Caminho ABSOLUTO do vídeo
        video_path = os.path.abspath(os.path.join("videos_salvos", entry.name))
        task.add_dataset(DataSet("VideosEntrada", [Element([video_path])]))

        # Metadados sistema
        task.add_dataset(DataSet("system_metadata2",
                       [Element([executed_by, cwd, hostname, python_version])]))

        # Processamento
        vc = cv2.VideoCapture(video_path)
        c = d = 0
        saved_frames = []
        timeF = 30

        rval = vc.isOpened()
        while rval:
            rval, frame = vc.read()
            if not rval:
                break
            if c % timeF == 0:
                ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                fname = f"{uuid4().hex}_{ts}.jpg"
                frame_path = frames_dir / fname

                if cv2.imwrite(str(frame_path), frame):
                    saved_frames.append(str(frame_path))
                    d += 1
                    if d >= 4:
                        break
            c += 1
            cv2.waitKey(1)
        vc.release()

        if saved_frames:
            print(f"🖼️  {len(saved_frames)} frames salvos para {entry.name}:")
            for i, p in enumerate(saved_frames):
                print(f"  [{i+1}] {p} (existe? {os.path.exists(p)})")

            # Adiciona um DataSet por frame (1 Element por vez)
            for i, p in enumerate(saved_frames):
                try:
                    dataset = DataSet("FramesGerados", [Element([p])])
                    task.add_dataset(dataset)
                    print(f"   ✅ Frame [{i+1}] registrado com sucesso.")
                except Exception as e:
                    print(f"   ❌ Erro ao registrar frame [{i+1}]:", e)
        else:
            print("⚠️ Nenhum frame salvo para este vídeo.")

        end_time = datetime.now().isoformat()
        task.add_dataset(DataSet("script_metadata2",
                       [Element([start_time, end_time, script_path, script_last_modified, script_hash])]))

        task.end()
        print(f"✔️ Task {count} finalizada.")
        count += 1
