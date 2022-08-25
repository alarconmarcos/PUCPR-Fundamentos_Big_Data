/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.implementacaomr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
 * @author marcos.alarcon
 */
public class implementacao3 {
        
    public static class MapperImplementacao3 extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 9 && 
                campos[4].equals("NARCOTICS") && 
                Integer.valueOf(campos[0]) %2 ==0) {
                
                String ano = campos[2];
                int ocorrencia = 1;
                
                Text chaveMap = new Text(ano);
                IntWritable valorMap = new IntWritable(ocorrencia);
                
                context.write(chaveMap, valorMap);
               
            }
        }
    }
    
    public static class ReducerImplementacao3 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for (IntWritable val : valores){
                soma += val.get();
            }
            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
        } 
        
    }
    
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OcorrenciasCriminais/ocorrencias_criminais_sample.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/marcos.alarcon/Desktop/implementacaoLocalMR/implementacao3";
        
        if (args.length == 2)   {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade3");
        
        job.setJarByClass(implementacao3.class);
        job.setMapperClass(MapperImplementacao3.class);
        job.setReducerClass(ReducerImplementacao3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true);
        
    }
    
}
