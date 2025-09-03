import requests
import os
import time
import socket
import hashlib
import getpass
import platform
from datetime import datetime
from dfa_lib_python.dataflow import Dataflow
from dfa_lib_python.transformation import Transformation
from dfa_lib_python.set import Set
from dfa_lib_python.set_type import SetType
from dfa_lib_python.attribute import Attribute
from dfa_lib_python.attribute_type import AttributeType
from dfa_lib_python.task import Task
from dfa_lib_python.dataset import DataSet
from dfa_lib_python.element import Element

# === CONFIGURAÇÃO DO DATAFLOW === #
dataflow_tag = "download_videos_niteroi"
dflow = Dataflow(dataflow_tag)

# TRANSFORMAÇÃO PRINCIPAL
transf = Transformation("DownloadVideos")

# Conjuntos de entrada e saída
transf_input = Set("iDownloadVideos", SetType.INPUT, [
    Attribute("camera_id", AttributeType.TEXT)
])
transf_output = Set("oDownloadVideos", SetType.OUTPUT, [
    Attribute("video_path", AttributeType.FILE)
])

# Metadados do sistema
system_metadata = Set("system_metadata1", SetType.INPUT, [
    Attribute("executed_by", AttributeType.TEXT),
    Attribute("cwd", AttributeType.TEXT),
    Attribute("hostname", AttributeType.TEXT),
    Attribute("python_version", AttributeType.TEXT)
])

# Metadados do script
# Metadados do script
script_metadata = Set("script_metadata1", SetType.INPUT, [
    Attribute("start_time", AttributeType.TEXT),
    Attribute("end_time", AttributeType.TEXT),
    Attribute("script_path", AttributeType.TEXT),
    Attribute("script_last_modified", AttributeType.TEXT),
    Attribute("script_hash", AttributeType.TEXT)
])


# Associando ao dataflow
dtransf_sets = [transf_input, transf_output, system_metadata, script_metadata]
transf.set_sets(dtransf_sets)
dflow.add_transformation(transf)
dflow.save()

# === CONFIGURAÇÕES === #
SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_LAST_MODIFIED = datetime.fromtimestamp(os.path.getmtime(SCRIPT_PATH)).isoformat()
SCRIPT_HASH = hashlib.sha256(open(SCRIPT_PATH, 'rb').read()).hexdigest()

EXECUTED_BY = getpass.getuser()
CWD = os.getcwd()
HOSTNAME = socket.gethostname()
PYTHON_VERSION = platform.python_version()

# === DICIONÁRIO DE CÂMERAS === #
cameras = {
    "000017": "FelicianoSodre",
    "000016": "Arariboia",
    "000020": "RioBrancoXVNovembro",
    "000027": "RuaConceicao",
    "000018": "JansenMeloDeodoro",
    "000019": "Benjamin",
    "000021": "AlamedaJoaoBrasil",
    "000022": "Getulinho",
    "000011": "PraiaFlexas",
    "000013": "PracaGetulioVargas",
    "000010": "RobertoSilveiraMiguelFrias",
    "000023": "GaviaoPeixoto",
    "000015": "LargoMarrao",
    "000014": "RuaSantaRosa",
    "000009": "RobertoSilveiraAryParreiras",
    "000012": "PraiaMarizBarros",
    "000008": "SkatePark",
    "000644": "Charitas",
    "000622": "ViarioCafuba",
    "000002": "CISPNitTrans",
    "000028": "TrevoCamboinhas",
    "000001": "Itaipu",
    "000007": "LargoBatalha",
    "000003": "TunelRaulVeiga",
    "000004": "TunelRobertoSilveira",
    "000006": "Forum",
    "000005": "ParqueColina",
    "000034": "FelicianoSodreMergulhao",
    "000035": "AryParreirasIrineuMarinho",
    "000036": "HospitalIcarai",
    "000030": "MiguelFrias",
    "000031": "PracaRadioAmador",
    "000029": "QuintinoBocaiuva",
    "000210": "RobertoSilveiraDominguesSa",
    "000606": "TunelCharitasCafuba",
    "000026": "JansenMeloPonte"
}

# === LOOP DE COLETA === #
count = 1
while True:
    for chave, valor in cameras.items():
        url = f'https://appnittrans.niteroi.rj.gov.br:8888/{chave}/last_video.mp4'
        print(f"[INFO] Tentando baixar: {url}")
        
        try:
            r = requests.get(url, allow_redirects=True, timeout=30)
            size_content = len(r.content)
            print(f"[INFO] Tamanho do vídeo baixado: {size_content/1024:.2f} KB")

            if r.status_code == 200 and size_content > 100_000:  # mínimo 100 KB para aceitar
                save_dir = '/mnt/d/Doutorado/ProvInCiA/Chuvas/Evaluation/videos_salvos'
                os.makedirs(save_dir, exist_ok=True)
                video_path = f"{save_dir}/{valor}{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
                
                with open(video_path, 'wb') as f:
                    f.write(r.content)

                print(f"✅ Vídeo salvo: {video_path}")

                # Proveniência
                                # Proveniência
                task = Task(count, dataflow_tag, "DownloadVideos")
                task.begin()
                start_time = datetime.now().isoformat()

                task.add_dataset(DataSet("iDownloadVideos", [Element([valor])]))
                task.add_dataset(DataSet("oDownloadVideos", [Element([video_path])]))
                task.add_dataset(DataSet("system_metadata1", [Element([
                    EXECUTED_BY, CWD, HOSTNAME, PYTHON_VERSION
                ])]))

                end_time = datetime.now().isoformat()
                task.add_dataset(DataSet("script_metadata1", [Element([
                    start_time, end_time, SCRIPT_PATH, SCRIPT_LAST_MODIFIED, SCRIPT_HASH
                ])]))

                task.end()


                count += 1
            else:
                print(f"[ERRO] Vídeo de {valor} não salvo: tamanho insuficiente ou erro HTTP ({r.status_code})")

            time.sleep(1)

        except Exception as e:
            print(f"[ERRO] Exceção ao tentar baixar {valor}: {e}")

    print("[INFO] Aguardando próximo ciclo de verificação...")
    time.sleep(600)  # 10 minutos

