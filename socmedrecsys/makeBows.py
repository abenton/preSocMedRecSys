'''
Contains code for building bags of words from a collection of tweets.

Adrian Benton
2/8/2013
'''

import codecs, cPickle, gzip, os, re, sys, time

replRe = re.compile('\W+')

def _tokenize(line):
  '''
  Simple tweet tokenizer.
  '''
  toks = line.lower().split()
  toks = [t for t in toks if not ((t.startswith('#')) or
                                  (t.startswith('@')) or 
                                  ('.ly' in t) or
                                  ('http' in t) or
                                  ('www' in t))]
  toks = [replRe.sub('', t) for t in toks if replRe.sub('', t)]
  return toks

def mkBows(tweetPath, outDir, handles, userToFold, numFolds=10, bigrams=False):
  '''
  Make bags of words for 
  '''
  tokenMapping = {}
  tokIdx = 0
  
  try:
    os.mkdir(outDir)
  except:
    pass
  
  folds = range(numFolds)
  for fold in folds:
    print 'Without fold %d' % (fold)
    bows = dict([(h, {}) for h in handles])
    tweetCounts = dict([(h, 0) for h in handles])
    
    now = time.time()
    
    lineIdx = 0
    bad = 0
    inFile = open(tweetPath)
    for line in inFile:
      lineIdx += 1
      if not lineIdx % 100000:
        print '%d * 100K, Bad: %d, Time: %f' % (lineIdx/100000,
                                               bad, time.time() - now)
      
      flds = [fld.strip() for fld in line.lower().split('\t')]
      try:
        user, shows, txt = int(flds[0]), eval(flds[1]), flds[2]
      except:
        bad += 1
        continue
      
      toks = _tokenize(txt)
      
      # If bigrams are turned on, then include them in the BOWs
      if bigrams:
        toks = ['%s %s' % (t1, t2) for t1, t2 in zip(toks, toks[1:])]
      
      try:
        for show in shows:
          for t in toks:
            if t not in tokenMapping:
              tokIdx += 1
              tokenMapping[t] = tokIdx
          if user in userToFold and show in handles and userToFold[user] != fold:
            for t in toks:
              tIdx = tokenMapping[t]
              
              if tIdx not in bows[show]:
                bows[show][tIdx] = 0
              tweetCounts[show] += 1
              bows[show][tIdx]  += 1
      except Exception, ex:
        print ex
    inFile.close()
    
    # Dumps the bag of words.
    bowFile = open(os.path.join(outDir, 'bows_wo%d.pickle' % (fold)), 'w')
    cPickle.dump(bows, bowFile)
    bowFile.close()
    
    # Dumps the number of tweets associated with each handle out.
    tweetCountFile = open(os.path.join(outDir,
                                       'tweetCounts_wo%d.pickle' % (fold)), 'w')
    cPickle.dump(tweetCounts, tweetCountFile)
    tweetCountFile.close()
    
    # Dumps the dictionary mapping to an output file.  First row are the
    # tokens, second row are the indices.
    dictMappingFile = codecs.open(os.path.join(outDir, 'dictionary.tsv'),
                                  'w', encoding='ascii', errors='ignore')
    mapping = sorted([(v, k) for k, v in tokenMapping.items()])
    for v, k in mapping:
      dictMappingFile.write(k + '\t')
    dictMappingFile.write('\n')
    for v, k in mapping:
      dictMappingFile.write(str(v) + '\t')
    dictMappingFile.close()

def toSparseAdjList(inPath, outPath, handles):
  '''
  Converts a bag of words to a sparse adjacency list format.
  '''
  inFile = open(inPath)
  bows = cPickle.load(inFile)
  inFile.close()
    
  outFile = open(outPath, 'w')
  now = time.time()
  for idx, handle in enumerate(handles):
    if not idx % 100:
      print idx, handle, time.time() - now
    bow = bows[handle]
    outFile.write(handle)
    for k in sorted(bow.keys()):
      outFile.write(',%d,%d' % (k, bow[k]))
    outFile.write('\n')
  outFile.close()

def toSparseAdjListAll(inDir, outDir, handles):
  '''
  Converts all bags of words in a directory to a sparse CSV format
  in an output directory.
  '''
  try:
    os.mkdir(outDir)
  except:
    pass
  
  bowPaths = [p for p in os.listdir(inDir) if p.startswith('bows')]
  for p in bowPaths:
    toSparseAdjList(os.path.join(inDir, p),
                    os.path.join(outDir, 
                                 p.replace('.pickle', '.csv')), handles)

def test():
  pass

if __name__ == '__main__':
  test()
