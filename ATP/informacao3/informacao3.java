/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @autores Marcos Alarcon
 *          Wallace Entringer Bottacin
 *          Bruno Otávio Barbosa Bordignon
 */

public class informacao3 {
    
    public static class MapperInformacao3 extends Mapper<Object, Text, Text, IntWritable>{
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String ano = campos[1];
                int ocorrencia = 1;
                
                Text chaveMap = new Text(ano);
                IntWritable valorMap = new IntWritable(ocorrencia);
                
                context.write(chaveMap, valorMap);
                
            }
        }
        
    }
    
    public static class ReducerInformacao3 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for (IntWritable val: valores){
                soma += val.get();
            }
            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
            
          
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
//        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/marcos.alarcon/Desktop/ATP/informacao3";

        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao3");
        
        job.setJarByClass(informacao3.class);
        job.setMapperClass(MapperInformacao3.class);
        job.setReducerClass(ReducerInformacao3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        //Exclui arquivo de saída se existir
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(arquivoSaida)))
            hdfs.delete(new Path(arquivoSaida), true);

        job.waitForCompletion(true);
        
      
        
    }
    
}
