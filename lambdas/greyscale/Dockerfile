FROM public.ecr.aws/lambda/python:3.13

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system libraries that Pillow (PIL) needs to build
RUN dnf install -y libjpeg-turbo-devel zlib-devel libtiff-devel freetype-devel

# Set the working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# copy requirements file
COPY pyproject.toml .

# create a virtual env AND install Python packages
RUN /bin/uv venv .venv && \
    /bin/uv pip install . --no-cache-dir

# handles "No module named 'handler'" error
COPY handler.py .

# set PYTHONPATH to include the virtual environment site-packages
ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/.venv/lib/python3.13/site-packages"

# set the CMD to handler
CMD ["handler.greyscale_handler"]