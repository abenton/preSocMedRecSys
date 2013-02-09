'''
Entrypoint for the Twitter recsys processing code.  Usage:

python procmain.py <PYTHON_CONFIG_MODULE>

Not providing a module will result in the test suite being run (loading from
test_config.py)

Adrian Benton
2/8/2013
'''

import os, sys
import preprocess as pp
import makeBows as mb

def main(moduleName):
  # Dynamically load constants from the configuration file.
  m = __import__(moduleName, fromlist=['*'])
  
  try:
    os.mkdir(m.OUTPUT_DIR)
  except:
    pass
  
  # Paths to files dumped to output directory
  netPath   = os.path.join(m.OUTPUT_DIR, m.NETWORK_NAME)
  tblPath   = os.path.join(m.OUTPUT_DIR, m.TWEET_TABLE_NAME)
  foldPath  = os.path.join(m.OUTPUT_DIR, 'folds')
  bowDir    = os.path.join(m.OUTPUT_DIR, m.BOW_DIR_NAME)
  tsvBowDir = os.path.join(m.OUTPUT_DIR, m.TSV_BOW_DIR_NAME)
  
  
  handles = pp.ldHandles(m.HANDLE_PATH)
  print 'Loaded handles'
  net = pp.buildNet(m.NETWORK_DIR, handles, netPath)
  print 'Built network'
  pp.netToAdjList(net, handles, netPath.replace('.pickle', '.tsv'))
  print 'Dumped network to adjacency list format'
  pp.buildTweetTable(m.TWEET_DIRS, net, tblPath)
  print 'Built tweet table'
  pp.genFolds(tblPath, foldPath, m.NUM_FOLDS)
  userToFold = pp.ldFolds(foldPath)
  print 'Got folds'
  
  if m.RAND_DIST_TWEETS:
    pp.randDistTweets(tblPath, tblPath.replace('.tsv', '.tmp'))
    os.rename(tblPath.replace('.tsv', '.tmp'), tblPath)
    print 'Randomly distributed tweets between handles'
  
  mb.mkBows(tblPath, bowDir, handles, userToFold,
                   m.NUM_FOLDS, m.INCLUDE_BIGRAMS)
  print 'Generated bags of words'
  
  mb.toSparseAdjListAll(bowDir, tsvBowDir, handles)
  print 'Dumped bags of words as adjacency list'

if __name__ == '__main__':
  moduleName = sys.argv[1] if len(sys.argv) > 1 else 'test_config'
  
  if len(sys.argv) < 2:
    print 'Running test configuration from test_config.py.  Usage: python procmain.py <PYTHON_CONFIG_MODULE>'
  
  main(moduleName)
  
  if len(sys.argv) < 2:
    print 'Finished running demo'
  
