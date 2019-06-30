import json,csv,time,pymongo,re,threading,sys

start = time.time()

# To skip initial number of rows to skip
try:
  skip = int(sys.argv[1])
except:
  skip = 0

def output_writer(row):
  # write row in csv file
  dest_csv = open("output.csv", "a")
  writer = csv.writer(dest_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
  writer.writerow(row)
  dest_csv.close()

def debug_writer(num):
  # write debug
  debugger = open("debug.log","a")  
  print(num,time.time()-start,sep="\t", file = debugger)
  debugger.close()

def error_writer(num,term,e):
  # write error
  debugger = open("err.log","a")  
  print(num,term,sep="\t", file = debugger)
  print(e, file = debugger)
  debugger.close()

def sub_thread_func(query,term,qType,count):  
  # Sub thread handles mongo queries
  url = "localhost"
  client = pymongo.MongoClient("mongodb://{}:27017/".format(url))
  db = client["iHOP"]
  col = db["articles"]
  try:
    mongoRes = col.find({query : {'$regex' : term}})
    count[qType] = mongoRes.count()
  except:
    count[qType] = mongoRes.count()
  finally:
    client.close()

def thread_func(row,num):
  # Main thread handles single row
  try:
    query = row[0].lower()
    count = [0,0,0,0]
    rgex = re.compile(query, re.IGNORECASE)
    try:
      res1 = threading.Thread(target=sub_thread_func,args=("extracted_information.participant_b.entity_text",rgex,0,count,))
      res2 = threading.Thread(target=sub_thread_func,args=("extracted_information.participant_a.entity_text",rgex,1,count,))
      res3 = threading.Thread(target=sub_thread_func,args=("extracted_information.participant_b.identifier",rgex,2,count,))
      res4 = threading.Thread(target=sub_thread_func,args=("extracted_information.participant_a.identifier",rgex,3,count,))
      res1.start()
      res2.start()
      res3.start()
      res4.start()
    except Exception as e:
      error_writer(num,row[0],e)
      return
    res1.join()
    res2.join()
    res3.join()
    res4.join()
    row.append(count[0]+count[1])
    row.append(count[2]+count[3])
  except Exception as e:
    error_writer(num,row[0],e)
  finally:
    debug_writer(num)
    output_writer(row)

thread_limit = 10 # Maximum number of threads at a time
threadCount = 0
threads = [None] * thread_limit
row_count=0
with open("queries.csv","r") as f:
  data = csv.reader(f)
  for row in data:
    if row_count<skip:
      row_count += 1
      continue
    try:
      if(int(row[1])>10):
        try:
          # Start row thread
          threads[threadCount] = threading.Thread(target=thread_func,args=(row,row_count,))
          threads[threadCount].start()
          threadCount += 1
        except Exception as e:
          error_writer(row_count,row[0],e)
      else:
        output_writer(row)    
    except Exception as e:
      error_writer(row_count,row[0],e)
    finally:
      if threadCount == thread_limit:
        # to limit maximum threads at a time
        print("Waiting ", (row_count+1))
        for x in threads:
          x.join()
        threadCount = 0
      row_count += 1

  