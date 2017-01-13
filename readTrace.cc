#include<stdio.h>
#include<iostream>
#include<mutex>
#include<chrono>
#include<condition_variable>
#include<cstdlib>
#include<thread>  
#include<string>
#include<leveldb/db.h>
#include<assert.h>
#include<list>
#include<sys/time.h>
#include<unistd.h>
#include"buildRandomValue.h"
#include "stdlib.h"     // srand(), rand()
#include "string.h"
#include<leveldb/db.h>
#include <leveldb/filter_policy.h>
#define MAX_VALUE_SIZE  (65536)

#define FAILED_VALUE -2
#define KEY_SIZE 24
static RandomGenerator rdgen;
static unsigned long long record_count = 0;
static unsigned long long load_count = 0;
static unsigned long long write_average_latency;
static unsigned long long read_average_latency;
static unsigned long long read_count;
static unsigned long long error_count;
leveldb::DB *db;
leveldb::Options ops;
leveldb::Status status;

void readTraceAndProcess(FILE *trace_file){
  std::string key,value;
  char line[200];
  char operation;
  char *pos;
  int length;
  unsigned long long diff;
  struct timeval start_pro_time;
  struct timeval start_time,end_time,res;
 
  gettimeofday(&start_pro_time,NULL);
 
  while(fscanf(trace_file,"%s",line) > 0){
    record_count++;
    if(record_count % 10000 == 0){
      fprintf(stderr,"\r record_count:%llu proceed:%.2lf%%",record_count,100*record_count*1.0/load_count);
      fflush(stderr);
    }

  
    //value length
    length=atoi(line);
    pos = strchr(line,',');
    pos++;
    //operation
    operation = *pos;
    pos = strchr(pos,',');
    pos++;
    //key
    key.assign(pos,KEY_SIZE);
 
    //value
   if(length > 0){// read hava no values
	value=rdgen.Generate(length).ToString();
   }
   
   gettimeofday(&start_time,NULL);
   if(operation == 'R'){
     status = db->Get(leveldb::ReadOptions(),key, &value);
 //    resFlag = kvserver->getValue(key,keyLength,readValue,readValueLength);
     
    }else{
      status = db->Put(leveldb::WriteOptions(),key,value);
    }
    if(!status.ok()){
      cout<<"record_count "<<record_count<<" error in key:"<<key<<"operation"<<operation<<endl;
    }
    gettimeofday(&end_time,NULL);
    timersub(&end_time,&start_time,&res);
    diff = res.tv_sec * 1000000 + res.tv_usec;
    
   
      if(operation == 'R'){
	read_count++;
	read_average_latency += diff;
      }else{
	write_average_latency += diff;
      }
    
  }
}

void init(char filename[],char load_count_str[],char dbfilename[],FILE **fp){

  int bloomBits;
  unsigned long long tableCacheSize;
  int compressionFlag;
  *fp = fopen(filename,"r");
  if(!fp){
    printf("error in open");
  }
  error_count = 0;
  read_count=0;
  record_count=0;
  read_average_latency = 0;
  write_average_latency = 0;
  
  ops.create_if_missing = true;
  fprintf(stderr,"please input bloom filter bits || Compression?1(true) or 0(false) ||  tableCache size\n");
  scanf("%d %d %llu",&bloomBits,&compressionFlag,&tableCacheSize);
  
  ops.filter_policy = leveldb::NewBloomFilterPolicy(bloomBits);
  if(!compressionFlag){
    ops.compression = leveldb::kNoCompression;   
  }
  ops.max_open_files = tableCacheSize;

  printf("environment:\n");
  printf("bloomfilterbits\tCompression\ttableCacheSize\t\n");
  printf("%15d\t%11s\t%14llu\t\n",bloomBits,compressionFlag?"true":"false",tableCacheSize);
  printf("filename:%s\t dbfilename:%s\n",filename,dbfilename?dbfilename:"testdb");
  load_count = strtoul(load_count_str,NULL,10);
   if(dbfilename == NULL){
    leveldb::Status status = leveldb::DB::Open(ops,"testdb",&db);
  }else{
    leveldb::Status status = leveldb::DB::Open(ops,dbfilename,&db);
  }
  if( fp == NULL ){
    perror("error\n");
  }
  fprintf(stderr,"> Beginning of test\n");
  

}

void output(){
  unsigned long long run_count = record_count;
  unsigned long long write_count = run_count - read_count;
  double wal = write_average_latency*1.0/write_count;
  double ral = read_average_latency*1.0/read_count;
  printf("error_count:%llu \n",error_count);
  printf("record_count:%llu run_count:%llu read_count:%llu  \n",record_count,run_count,read_count);
  printf("%s:%.2lfus \n %s:%.2lfus\n","write_average_latency",wal,"read_average_latency",ral);
}

int main(int argc,char *argv[]){

  FILE *fp = NULL;
  init(argv[1],argv[2],argv[3],&fp);
  readTraceAndProcess(fp);
 
  output();
  return 0;
}
