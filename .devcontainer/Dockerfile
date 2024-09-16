# DevContainer image
FROM python:3.9-slim

RUN \
    adduser --system --disabled-password --shell /bin/bash --home /home/vscode vscode && \
    apt-get clean

RUN \
    # dev setup
    apt update && \
    apt-get install sudo jq git bash-completion -y && \
    pip install --no-cache-dir --upgrade pip flake8 black faker ipykernel && \
    usermod -aG sudo vscode && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    echo '. /etc/bash_completion' >> /home/vscode/.bashrc && \
    echo 'export PS1="\[\e[32;1m\]\u\[\e[m\]@\[\e[34;1m\]\H\[\e[m\]:\[\e[33;1m\]\w\[\e[m\]$ "' >> /home/vscode/.bashrc && \
    apt-get clean

RUN \
    # networkx + plot(matplotlib) + dot(graphviz)
    sudo apt-get install gcc graphviz libgraphviz-dev -y && \
    pip install --no-cache-dir networkx[default] matplotlib pygraphviz && \
    # install PyQt5 for GUI windows (supported in VSCode debugger)
    sudo apt-get install -y '^libxcb.*-dev' libx11-xcb-dev libglu1-mesa-dev libxrender-dev libxi-dev libxkbcommon-dev libxkbcommon-x11-dev && \
    pip install --no-cache-dir pyqt5 && \
    # add pydantic for model structure, yaml parsing, and pytests
    pip install --no-cache-dir pydantic pyyaml pytest && \
    apt-get clean

USER vscode
ENV PATH ${PATH}:/home/vscode/.local/bin

CMD tail -f /dev/null