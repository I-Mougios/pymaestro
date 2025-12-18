# Maestro Example: Image Processing Pipeline

This example demonstrates how **Maestro** can be used to orchestrate a small but realistic workflow consisting of:

1. Downloading images asynchronously  
2. Processing them in parallel using multiple CPU cores  
3. Cleaning up intermediate artifacts  
4. Persisting and restoring the workflow definition  

The goal is to showcase **different ways to define workflows**, not just how to execute them.  
Furthermore, it demonstrates the variety of ways that jobs can be defined — from regular functions, async functions, and standalone scripts — and how jobs can explicitly depend on one another.

---

## What this example does

The pipeline consists of three logical steps:

1. **Download images**  
   Images are fetched concurrently using an async job.

2. **Remove image backgrounds**  
   CPU-bound image processing is executed in parallel using a process pool.

3. **Cleanup**  
   A standalone script removes intermediate files.

These steps form a **directed workflow**, where later jobs depend on the results of earlier ones.

---

## Directory structure

```text
docs/01_example/
├── common.py (shared domain logic)
├── decorator_api.py
├── programmatic_orchestration.py
├── maestro_serialize.py
├── cleanup.py
```

### Multiple ways to define the same workflow

This example intentionally demonstrates three equivalent ways to describe the same pipeline:

| Approach        | File                              |
|-----------------|-----------------------------------|
| Decorator-based | `decorator_api.py`                |
| Programmatic    | `programmatic_orchestration.py`   |
| Serialized      | `maestro_serialize.py`            |


Each approach serves a different purpose:

 - decorators for readability

 - programmatic construction for flexibility

 - serialization for portability and reproducibility

## Declaring dependencies explicitly

Instead of relying on implicit execution order, Maestro allows dependencies to be expressed explicitly using `DependsOn`:

```python
args=(DependsOn("download_images"),)
```

This makes workflows:

 - easier to reason about

 - safer to refactor

 - portable and serializable

 - explicit in their data flow

## Why serialization matters

Workflows are often defined in one place but executed in another.

Serialization allows a Maestro workflow to be:

 - defined once

 - persisted to disk

 - rehydrated later

 - executed in a different process or environment

This enables use cases such as:

 * separating workflow definition from execution

 * shipping workflows across machines or containers

 * versioning workflow definitions

 * inspecting workflows without executing them

The file `maestro_serialize.py` illustrates how a complete workflow can be saved as a JSON artifact.

**Running the examples**

Each file can be executed independently:

``` bash
    python decorator_api.py
    python programmatic_orchestration.py
    python maestro_serialize.py
```

After serialization, the generated JSON file can be loaded and executed using Maestro’s CLI or API.

```bash
    maestro shell
    maestro deserialize image_jobs.json
    maestro execute
```