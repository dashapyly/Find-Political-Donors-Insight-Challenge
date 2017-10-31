import os.path as op
import sys

SRC_DIR = op.join(op.dirname(op.dirname(op.abspath(__file__))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)