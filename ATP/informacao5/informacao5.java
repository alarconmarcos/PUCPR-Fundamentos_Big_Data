/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.NumberFormat;
import java.util.Locale;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @autores Marcos Alarcon
 *          Wallace Entringer Bottacin
 *          Bruno Otávio Barbosa Bordignon
 */

public class informacao5 {
    static int maiorValor = 0;
    static String maiorMercadoria;
    
    public static class MapperInformacao5 extends Mapper<Object, Text, Text, IntWritable>{
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            IntWritable valorMap = new IntWritable(0);
            if (campos.length == 10 && 
                campos[1].equals("2016")){
                String mercadoria = campos[3];
                String vlr = campos[5];
                
                try{
                    valorMap = new IntWritable(Integer.parseInt(vlr));
                }catch(NumberFormatException e){
                    
                }finally{
                    
                }
                    
                Text chaveMap = new Text(mercadoria);
                context.write(chaveMap, valorMap);
            }
        }
        
    }
    
    public static class ReducerInformacao5 extends Reducer<Text, IntWritable, Text, LongWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for (IntWritable val: valores){
                soma += val.get();
            }
            LongWritable valorSaida = new LongWritable(soma);
            context.write(chave, valorSaida);
            if(soma > maiorValor){
                maiorValor = soma;
                maiorMercadoria = chave.toString();
                
            }
            
          
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
//        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";

        String arquivoSaida = "/home2/ead2022/SEM1/marcos.alarcon/Desktop/ATP/informacao5";

        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao5");
        
        job.setJarByClass(informacao5.class);
        job.setMapperClass(MapperInformacao5.class);
        job.setReducerClass(ReducerInformacao5.class);
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
       
        System.out.print("A Mercadoria com maior quantidade de transações financeiras é: "+maiorMercadoria+ ", sendo um total de "+NumberFormat.getCurrencyInstance(meuLocal).format(maiorValor));

      
        
    }
    
}
