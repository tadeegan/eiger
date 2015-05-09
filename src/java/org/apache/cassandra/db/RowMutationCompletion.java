package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.commons.math3.distribution.NormalDistribution;

public class RowMutationCompletion implements ICompletable
{
    private final Message message;
    private final String id;
    private final RowMutation rm;
    
    private long startTime;

    private int maxMutationDelay = 0; //default to 0 if the environment var is not set
    private static Logger logger = LoggerFactory.getLogger(RowMutationCompletion.class);
    
    public RowMutationCompletion(Message message, String id, RowMutation rm)
    {
    	this.startTime = System.currentTimeMillis();
    	
        this.message = message;
        this.id = id;
        this.rm = rm;
        
        String value = System.getenv("max_mutation_delay_ms");
        try{
        	this.maxMutationDelay = Integer.parseInt(value);
        	
        }
        catch(NumberFormatException e){
        	logger.error(e.getLocalizedMessage());
        }
    }

    // Complete the blocked RowMutation
    @Override
    public void complete()
    {
    	long endTime = System.currentTimeMillis();
    	long deltaTime = endTime - this.startTime;
    	logger.debug("~~~~ [DEEGAN] Completion Complete (" + deltaTime + "ms )" );
    	try {
    		long delay = (long) getDelay();
    		Thread.sleep(delay);
    		logger.debug("done....");
    	}catch (Exception interupt) {
    		
    	}
        RowMutationVerbHandler.instance().applyAndRespond(message, id, rm);
    }

    private double getDelay() {
    	long time = (long)(Math.random()*this.maxMutationDelay);
		logger.debug("Max mutation delay: " + this.maxMutationDelay);
		logger.debug("time: " + time);
		NormalDistribution dist = new NormalDistribution(0, time);
		return Math.abs(dist.sample());
	}

}
