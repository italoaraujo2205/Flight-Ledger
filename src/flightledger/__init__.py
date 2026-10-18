"""Flight operations, source revisions and reproducible data products."""
import os

os.environ.setdefault("RAYON_NUM_THREADS", "2")
os.environ.setdefault("TOKIO_WORKER_THREADS", "2")
__version__ = "0.1.0"
