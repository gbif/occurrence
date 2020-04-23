import argparse, pandas as pd, numpy

if __name__ == '__main__':

  #arguments parsing
  parser = argparse.ArgumentParser()
  parser.add_argument("file1", help="File 1 to compare")
  parser.add_argument("file2", help="File 2 to compare")
  parser.add_argument("outFile", help="File to store the differences")
  args = parser.parse_args()

  fileOne = pd.read_csv(args.file1, index_col=None, sep='\t', dtype=str)
  fileTwo = pd.read_csv(args.file2, index_col=None, sep='\t',  dtype=str)

  with open(args.outFile, 'w') as outFile:
    outFile.write('\t' + '\t'.join(fileOne.columns) + '\n')
    outFile.write(args.file1 + '\t' + '\t'.join(map(lambda col:str(fileOne[col].nunique()), fileOne.columns)) + '\n')
    outFile.write(args.file2 + '\t' + '\t'.join(map(lambda col:str(fileTwo[col].nunique()), fileOne.columns)) + '\n')
    outFile.write('\t' + '\t'.join(map(lambda col:str(fileOne[col].nunique() - fileTwo[col].nunique()), fileOne.columns)) + '\n')
