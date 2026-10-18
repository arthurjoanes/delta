# Documentation scripts

This directory contains scripts to build https://docs.delta.io and the Scala, Java, and Python API references.

## Set up the environment

### Install Node environment

Install Node.js v22.18.0, as specified in `.nvmrc` and `package.json`, using [nvm](https://github.com/nvm-sh/nvm):

```
nvm install
```

Then, install [pnpm](https://pnpm.io/):

```
npm install --global corepack@latest
corepack enable pnpm
```

Install dependencies:

```
pnpm i
```

### Install Conda environment

Follow [Conda Download](https://www.anaconda.com/download/) to install Anaconda.

Create the `delta_docs` environment from `docs/environment.yml`, then activate it. See [Conda's environment guide](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-from-file) for activation instructions.

```
# Note the `--file` argument should be a fully qualified path. Using `~` in file
# path doesn't work. Example valid path: `/Users/macuser/delta/docs/environment.yml`

conda env create --name delta_docs --file=<absolute_path_to_delta_repo>/docs/environment.yml
```

### JDK Setup

API reference generation requires JDK 1.8. Set `JAVA_HOME` to its installation directory.

### Set the Delta Lake version

Set the Delta Lake release version to document:

```
export _DELTA_LAKE_RELEASE_VERSION_=3.3.0
```

## Usage

Run the command from the `delta` repo root directory:

```
python3 docs/generate_docs.py --livehtml --api-docs
```

The command prints a URL for the local preview.

### Skip generating API docs

For a faster preview without generating the API references, run:

```
python3 docs/generate_docs.py --livehtml
```

### Building for production

Build the documentation files:

```bash
python3 docs/generate_docs.py --api-docs
```

The output is written to `docs/dist`.

### Additional docs site commands

The site uses [Astro](https://astro.build/). Run these commands from `docs`:

| Command               | Description                                     |
| --------------------- | ----------------------------------------------- |
| `pnpm run lint`       | Run ESLint on the docs site code                |
| `pnpm run format`     | Format docs site code using Prettier            |
| `pnpm run dev`        | Start Astro in development mode                 |
| `pnpm run build`      | Build the Astro site for production             |
| `pnpm run preview`    | Preview the built Astro site                    |
| `pnpm run astro`      | Run Astro CLI                                   |
