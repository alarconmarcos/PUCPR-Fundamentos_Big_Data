/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Locale;
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
public class informacao1 {
    static int maiorValor = 0;
    static String maiorPais;
    
    public static class MapperInformacao1 extends Mapper<Object, Text, Text, IntWritable>{
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String pais = campos[0];
                int ocorrencia = 1;
                
                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(ocorrencia);
                
                context.write(chaveMap, valorMap);
                
            }
        }
        
    }
    
    public static class ReducerInformacao1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for (IntWritable val: valores){
                soma += val.get();
            }
            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
                
            if(soma > maiorValor){
                maiorValor = soma;
                maiorPais = chave.toString();
             
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
//        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/marcos.alarcon/Desktop/ATP/informacao1";

        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao1");
        
        job.setJarByClass(informacao1.class);
        job.setMapperClass(MapperInformacao1.class);
        job.setReducerClass(ReducerInformacao1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        //Exclui arquivo de saída se existir
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(arquivoSaida)))
            hdfs.delete(new Path(arquivoSaida), true);

        job.waitForCompletion(true);
        
        Locale meuLocal = new Locale( "pt", "BR" );

        System.out.print("O País com a maior quantidade de transações comerciais é: "+maiorPais+ ", com um total de " +NumberFormat.getNumberInstance(meuLocal).format(maiorValor)+ " transações");

      
        
    }
    
}
