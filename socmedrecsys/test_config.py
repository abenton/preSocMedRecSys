'''
This file just contains parameters that procmain.py uses to preprocess the
recsys Twitter data.  See comment above each parameter for more info.  Config
module is passed on the command line, and dynamically loaded.

Your config files should contain the same constants.  File paths should adhere
to the same extensions as seen below.
'''

from os.path import join 

# Will set the directory that output will be written to
OUTPUT_DIR  = 'test_output'

# Path to where the handles you want to consider are
HANDLE_PATH  = join('test', 'handles.txt')

# Directory where the handle network data are stored
NETWORK_DIR  = join('test', 'nets')

# Directories where the raw tweets are stored
TWEET_DIRS   = [join('test', p) for p in ['tweets1', 'tweets2']]

# Name of the output network file.  The name of the sparse form of this is
# the same but with "pickle" replaced with "tsv"
NETWORK_NAME = 'net.pickle'

# Name of the tweet table where processed tweets are dumped
TWEET_TABLE_NAME = 'tweetTable.tsv'

# Directory name where the pickled BOWs are stored
BOW_DIR_NAME = 'bows'

# Directory name where 
TSV_BOW_DIR_NAME = 'sparse_bows'

# Randomly distributes a user's tweets between all handles they follow
RAND_DIST_TWEETS = False

# Whether or not to include bigrams in the bag of words model
INCLUDE_BIGRAMS = False

# Number of folds to split your users into.
NUM_FOLDS = 10
