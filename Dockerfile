# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Single-stage build: install DVT from source into the image's system Python
# and run as a nonroot user. --no-cache keeps uv's build cache out of the image.

FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

RUN useradd --uid 65532 --create-home --shell /usr/sbin/nologin nonroot

WORKDIR /app

COPY . /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

RUN uv pip install --system --no-cache .

USER nonroot

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["data-validation"]
