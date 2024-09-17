from pydantic import BaseModel
from typing import List, Dict, Any, Callable, Literal
from typing_extensions import TypedDict


# no mapper -> mapper
def default_map(parents, children, mapper):
    # hadamard (element-wise) file product
    # assumes N parents, 1 child, and parent order matches expected_collection order
    input_sets = [
        [f"{p_name}/{f}" for f in c.files]
        for p_name, p in parents.items()
        for c in p["collections"]
    ]
    collections = [{"files": list(c)} for c in zip(*input_sets)]
    expected_files = list(children.values())[0]["expected_collection"].files
    mapped_names = [
        {f: mapped_name for f, mapped_name in zip(c["files"], expected_files)}
        for c in collections
    ]
    return dict(collections=collections, mapped_names=mapped_names)


class Collection(BaseModel):
    env_vars: List[Dict[str, Any]] = []
    files: List[str] = []


class Mapper(BaseModel):
    method: Callable = default_map
    input_dir: str = "/input"
    output_dir: str = "/output"
    memory_limit: str = "100MiB"
    cpu_limit: float = 0.25
    modify_input: bool = False


class Input(BaseModel):
    collections: List[Collection]


class PodJob(BaseModel):
    expected_collection: Collection
    output_dir: str = "/output"
    memory_limit: str = "1GiB"
    cpu_limit: float = 1.0
    max_workers: int = 1


class PodRun(PodJob):
    collections: List[Collection]


class Container(BaseModel):
    name: str
    env_vars: Dict[str, Any] = {}
    file_mounts: Dict[
        str,
        TypedDict("Mount", {"mount_dest": str, "mount_mode": Literal["ro", "rw"]}),
    ] = {}
    dir_mounts: Dict[
        str,
        TypedDict("Mount", {"mount_dest": str, "mount_mode": Literal["ro", "rw"]}),
    ] = {}
    memory_limit: str
    cpu_limit: float


def simulate_orchestration(user_pipeline):
    raise Exception("Not implemented.")
