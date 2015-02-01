__author__ = 'Mark'

import os
from datetime import datetime

#ROOT = os.path.dirname(os.path.dirname(__file__))
ROOT = os.path.dirname(__file__)

FOUT_DIR = os.path.join(ROOT, 'OutputFiles')
FOUT_FILE = os.path.join(FOUT_DIR, '%s_output.txt' % datetime.now().strftime('%Y.%m.%d'))