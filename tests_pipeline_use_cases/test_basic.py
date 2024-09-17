from utils import (
    Collection,
    Mapper,
    Input,
    PodRun,
    Container,
    simulate_orchestration,
)


def test_combination():
    # combination style transfer: 2 images, 2 images, cartesian product, 4 workers

    def map_combine1(parents, children, mapper):
        # cartesian (combination) file product, assumes N parents and 1 child
        import itertools

        input_sets = [
            [f"{p_name}/{f}" for f in c.files]
            for p_name, p in parents.items()
            for c in p["collections"]
        ]
        collections = [{"files": list(c)} for c in itertools.product(*input_sets)]
        expected_files = list(children.values())[0]["expected_collection"].files
        # specific logic for name mapping
        map_name = lambda fname, options: (
            options[0] if "paint" in fname else options[1]
        )
        mapped_names = [
            {f: map_name(f, expected_files) for f in c["files"]} for c in collections
        ]
        return dict(collections=collections, mapped_names=mapped_names)

    user_pipeline = {
        "pipeline_settings": {
            "store": "/pipeline1",
            "cpu_limit": 16,
            "memory_limit": "64GiB",
        },
        "nodes": {
            "input1": Input(
                collections=[
                    Collection(
                        files=[
                            "a/b/c/paint-0.png",
                            "a/b/c/paint-1.png",
                        ]
                    )
                ]
            ),
            "input2": Input(
                collections=[
                    Collection(
                        files=[
                            "a/b/c/person-0.png",
                            "a/b/c/person-1.png",
                        ]
                    )
                ]
            ),
            "map1": Mapper(
                method=map_combine1,
            ),
            "pod1": PodRun(
                expected_collection=Collection(
                    files=["/main/painting.png", "/main/photo.png"]
                ),
                output_dir="/main/output",
                max_workers=4,
                collections=[
                    Collection(
                        files=[
                            "styled.png",
                        ]
                    )
                ],
            ),
        },
        "streams": """
            input1 -> map1
            input2 -> map1
            map1 -> pod1
        """,
    }
    expected_run_sequence = {
        0: {
            "containers": [
                Container(
                    name="map1",
                    memory_limit="100MiB",
                    cpu_limit=0.25,
                    file_mounts={
                        "/pipeline1/input1/a/b/c/paint-0.png": {
                            "mount_dest": "/input/a/b/c/paint-0.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-0.png": {
                            "mount_dest": "/input/a/b/c/person-0.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input1/a/b/c/paint-1.png": {
                            "mount_dest": "/input/a/b/c/paint-1.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-1.png": {
                            "mount_dest": "/input/a/b/c/person-1.png",
                            "mount_mode": "ro",
                        },
                    },
                )
            ],
            "store": """
            pipeline1:
              input1:
                a:
                  b:
                    c:
                      paint-0.png:
                      paint-1.png:
              input2:
                a:
                  b:
                    c:
                      person-0.png:
                      person-1.png:
            """,
        },
        1: {
            "containers": [
                Container(
                    name="pod1-w0",
                    memory_limit="1GiB",
                    cpu_limit=1,
                    file_mounts={
                        "/pipeline1/input1/a/b/c/paint-0.png": {
                            "mount_dest": "/main/painting.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-0.png": {
                            "mount_dest": "/main/photo.png",
                            "mount_mode": "ro",
                        },
                    },
                    dir_mounts={
                        "/pipeline1/pod1/w0": {
                            "mount_dest": "/main/ouput",
                            "mount_mode": "rw",
                        }
                    },
                ),
                Container(
                    name="pod1-w1",
                    memory_limit="1GiB",
                    cpu_limit=1,
                    file_mounts={
                        "/pipeline1/input1/a/b/c/paint-0.png": {
                            "mount_dest": "/main/painting.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-1.png": {
                            "mount_dest": "/main/photo.png",
                            "mount_mode": "ro",
                        },
                    },
                    dir_mounts={
                        "/pipeline1/pod1/w1": {
                            "mount_dest": "/main/ouput",
                            "mount_mode": "rw",
                        }
                    },
                ),
                Container(
                    name="pod1-w2",
                    memory_limit="1GiB",
                    cpu_limit=1,
                    file_mounts={
                        "/pipeline1/input1/a/b/c/paint-1.png": {
                            "mount_dest": "/main/painting.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-0.png": {
                            "mount_dest": "/main/photo.png",
                            "mount_mode": "ro",
                        },
                    },
                    dir_mounts={
                        "/pipeline1/pod1/w2": {
                            "mount_dest": "/main/ouput",
                            "mount_mode": "rw",
                        }
                    },
                ),
                Container(
                    name="pod1-w3",
                    memory_limit="1GiB",
                    cpu_limit=1,
                    file_mounts={
                        "/pipeline1/input1/a/b/c/paint-1.png": {
                            "mount_dest": "/main/painting.png",
                            "mount_mode": "ro",
                        },
                        "/pipeline1/input2/a/b/c/person-1.png": {
                            "mount_dest": "/main/photo.png",
                            "mount_mode": "ro",
                        },
                    },
                    dir_mounts={
                        "/pipeline1/pod1/w3": {
                            "mount_dest": "/main/ouput",
                            "mount_mode": "rw",
                        }
                    },
                ),
            ],
            "store": """
            pipeline1:
              input1:
                a:
                  b:
                    c:
                      paint-0.png:
                      paint-1.png:
              input2:
                a:
                  b:
                    c:
                      person-0.png:
                      person-1.png:
              pod1:
                w0:
                  styled.png:
                w1:
                  styled.png:
                w2:
                  styled.png:
                w3:
                  styled.png:
            """,
        },
    }

    assert simulate_orchestration(user_pipeline) == expected_run_sequence
