'''
Contains methods for pre-processing the tweet data (e.g., dumping it to a table,
building a network).  makeBows.py contains the code for counting tokens.

Adrian Benton
2/8/2013
'''

import codecs, cPickle, gzip, os, random, re, sys, time

def randDistTweets(inPath, outPath):
  '''
  Given a tweet table, makes sure that each tweet is associated with only
  one handle.
  '''
  inFile  = open(inPath)
  outFile = open(outPath, 'w')
  bad = 0
  good = 0
  
  oldHandles = []
  oldLines = []
  oldUid = -1
  totIdx = 0
  for line in inFile:
    totIdx += 1
    if not totIdx % 10000:
      print totIdx
    flds = line.split('\t')
    if len(flds) < 3:
      bad += 1
      continue
    uid = int(flds[0])
    if uid != oldUid:
      random.shuffle(oldLines)
      
      if oldHandles:
        for idx, oldLine in enumerate(oldLines):
          oldLine[1] = str([oldHandles[idx % len(oldHandles)]])
          outFile.write('\t'.join(oldLine))
      oldUid = uid
      
      try:
        oldHandles = eval(flds[1])
      except Exception, ex:
        bad += 1
        oldHandles = []
        continue
      
      random.shuffle(oldHandles)
      oldLines = [line.split('\t')]
      good += 1
    else:
      good += 1
      oldLines.append(flds)
  outFile.close()
  inFile.close()
  
  print good, bad

def ldHandles(handlePath):
  '''
  Loads a list of handles from a file.  Assumes one handle per line and
  nothing else.
  '''
  with open(handlePath) as f:
    handles = [line.strip().lower() for line in f if line.strip()]
  return handles

def ldFolds(foldDir):
  '''
  Given a directory of users per fold loads them into a map from user ID to
  fold number.
  '''
  pRe = re.compile('userSet_(?P<fold>\d+).txt')
  ps = [(int(pRe.match(p).group('fold')),
         os.path.join(foldDir, p)) for p in os.listdir(foldDir)]
  foldUsers = {}
  for fold, p in ps:
    f = open(p)
    for line in f:
      foldUsers[int(line.strip().split('\t')[0])] = fold
    f.close()
  return foldUsers

def buildNet(netDir, handles, netPath):
  '''
  Given a directory with product network files, builds the network, a
  dictionary mapping from the handles we care about to all followers of
  that handle.  Also dumps the pickled network to an output file.
  
  output: {handle:set(user_ids)}
  '''
  now = time.time()
  
  ps = [os.path.join(netDir, p) for p in os.listdir(netDir)
                                      if p.startswith('follower')]
  net = {}
  for p in ps:
    print 'Building network from', p, time.time() - now
    f = open(p)
    for line in f:
      try:
        flds = line.split('\t')
        h = flds[1].strip().lower()
        if h not in handles:
          continue
        if h not in net:
          net[h] = set()
        for u in flds[2:]:
          try:
            net[h].add(int(u))
          except:
            pass
      except:
        pass
    f.close()
  
  netFile = open(netPath, 'w')
  cPickle.dump(net, netFile)
  netFile.close()
  
  return net

def netToAdjList(net, handles, outPath):
  '''
  Converts a network of product followers to sparse adjacency list format.
  '''
  f = open(outPath, 'w')
  for h in handles:
    f.write(h)
    for u in sorted(list(net[h])):
      f.write('\t' + str(u))
    f.write('\n')
  f.close()

def _uFltr(tweets):
  '''
  filter for which users to keep in our tweet table and folds.
  '''
  if not tweets:
    return False
  else:
    last = tweets[-1]
    folls, lang = last.user.followers_count, last.user.lang
    return folls <= 2000 and lang == 'en'

def buildTweetTable(tweetDirs, net, tweetPath):
  '''
  Given multiple directories with users' previous tweets, constructs a TSV
  table of all tweets in our set.  Must pass directory where tweets are
  kept, a product network, and an output path. Format of the file:
  
  <USER_ID> <FOLLOWED_HANDLES> <TEXT>
  ''' 
  ps = [os.path.join(d, p) for d in tweetDirs for p in os.listdir(d)]
  total = len(ps)
  pIdx = 0
  now = time.time()
  bad = 0
  
  outFile = codecs.open(tweetPath, 'w', encoding='ascii', errors='ignore')
  
  print len(ps), 'tweet files to parse'
  
  goodCount = 0
  badCount = 0
  for p in ps:
    pIdx += 1
    if not pIdx % 200:
      print pIdx, badCount, total, time.time() - now
    
    try:
      f = gzip.open(p)
      tweets = cPickle.load(f)
      f.close()
      
      if _uFltr(tweets):
        uId = tweets[0].user.id
        prods = [prod for prod in net.keys() if uId in net[prod]]
        for t in tweets:
          txt = t.text.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
          outFile.write('%d\t%s\t%s\n' % (uId, str(prods), txt))
    except Exception, ex:
      print ex
      badCount += 1
  outFile.close()

def genFolds(tweetPath, foldDir, numFolds=10):
  '''
  Generates <b>numFolds</b> folds of users from the users in the tweet listing.
  Dumps these files as userSet_%d.txt to the directory <b>foldDir</b>, for
  each fold.
  '''
  
  try:
    os.mkdir(foldDir)
  except:
    pass
  
  users = set()
  outFiles = dict([(i, open(os.path.join(foldDir, 'userSet_%d.txt' % (i)), 'w'))
                                  for i in range(numFolds)])
  
  lineIdx = 0
  f = open(tweetPath)
  for line in f:
    lineIdx += 1
    if not lineIdx % 100000:
      print 'Iter %d * 100K' % (lineIdx/100000)
    users.add(int(line.split('\t')[0]))
  f.close()
  
  print 'Num users:', len(users)
  users = list(users)
  random.shuffle(users)
  
  for i, u in enumerate(users):
    outFiles[i%numFolds].write(str(u) + '\n')
  
  for f in outFiles.values():
    f.close()

def test():
  testDir = 'test'
  handlePath = os.path.join(testDir, 'handles.txt')
  netDir     = os.path.join(testDir, 'nets')
  tweetDirs  = [os.path.join(testDir, d) for d in ['tweets1', 'tweets2']]
  tweetPath  = os.path.join(testDir, 'tweetTable.tsv')
  netPath    = os.path.join(testDir, 'net.pickle')
  foldDir    = os.path.join(testDir, 'folds')
  
  print 'Testing preprocessing code'
  
  handles = ldHandles(handlePath)
  print 'Loaded handles'
  net = buildNet(netDir, handles, netPath)
  print 'Built network'
  buildTweetTable(tweetDirs, net, tweetPath)
  print 'Built tweet table'
  genFolds(tweetPath, foldDir, 5)
  print 'Got folds'
  
  print 'Check under "test" for "net.pickle", "tweetTable.tsv", and ' + \
        '"folds/userSet_0.txt" to "folds/userSet_5.txt"'
  resp = raw_input('Enter "y" to clean directory, else test output remains:\n')
  
  if resp.lower() == 'y':
    os.remove(tweetPath)
    os.remove(netPath)
    shutil.rmTree(foldDir)

if __name__ == '__main__':
  test()
