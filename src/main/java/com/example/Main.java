package com.example;

public class Main {
    public static void main(String[] args) throws Exception {
        // Plug in any job runner here
        //JobRunnerFirst job = new JobRunnerFirst();
        // JobRunnerSecond job = new JobRunnerSecond();
        JobRunnerThird job = new JobRunnerThird();
        job.run();
    }
    
}
