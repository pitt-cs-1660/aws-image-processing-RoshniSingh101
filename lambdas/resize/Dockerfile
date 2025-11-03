FROM public.ecr.aws/lambda/python:3.13 AS builder

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

############
#
# TODO: implement the rest of the image
#
############


WORKDIR /aws-image-processing-RoshniSingh101/lambdas/resize

# copy dependency files first (for better caching)
COPY pyproject.toml ./

RUN uv venv

# install dependencies without the project itself
RUN uv sync --no-install-project --no-editable

# Copy source code
COPY . .

# install the complete project
RUN uv sync --no-editable

# final stage - runtime environment
FROM public.ecr.aws/lambda/python:3.13

# install uvicorn directly if it's not in dependencies
RUN pip install uvicorn

# set up virtual environment variables
ENV VIRTUAL_ENV=/aws-image-processing-RoshniSingh101/lambdas/resize/.venv
ENV PATH="/aws-image-processing-RoshniSingh101/lambdas/resize/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# set working directory
WORKDIR /aws-image-processing-RoshniSingh101/lambdas/resize

# copy the virtual environment from builder
COPY --from=builder /aws-image-processing-RoshniSingh101/lambdas/resize/ /aws-image-processing-RoshniSingh101/lambdas/resize/

EXPOSE 8000

CMD ["handler.handler"]
