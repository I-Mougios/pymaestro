**pymaestro** is a lightweight Python orchestration framework for defining and executing tasks.
It supports multiple ways of interacting with the system:

- A **programmatic (functional) API** for explicit and dynamic job registration
- A **declarative, decorator-based API** for concise and readable task definitions
- A **CLI interface** for managing and executing jobs without writing orchestration code

All approaches are built on the same underlying execution model, allowing users to choose
the style that best fits their workflow.

---

### Why pymaestro is Lightweight

`pymaestro` is designed to be lightweight by construction.
It runs entirely in-process and does not require a scheduler daemon, persistent metadata store,
message broker, or external services. Tasks are explicitly added, inspected, and executed
by the user, making the execution model transparent and easy to reason about.

Unlike full-scale orchestration platforms that target distributed and long-running
pipelines, `pymaestro` focuses on simplicity, portability, and low operational overhead.
By deliberately limiting its scope, it covers the majority of common orchestration
use cases while remaining easy to build, deploy, and extend.

---

### Supported Functionality

Maestro currently supports a concise set of core orchestration features:

- Add tasks from scripts, Python callables, or async callables
- Assign optional names, arguments, and parallel execution groups
- Remove scheduled tasks by name or insertion order
- Inspect the current execution plan and task ordering
- Execute tasks sequentially or in parallel, depending on grouping
- Serialize and deserialize the orchestration state to and from JSON
- Interact via a CLI or an interactive shell for iterative workflows


### Installation

To install the **core library** from PyPI:

```bash
  pip install python-maestro
```

To install the core library with CLI support:
```bash
  pip install python-maestro[cli]
```

