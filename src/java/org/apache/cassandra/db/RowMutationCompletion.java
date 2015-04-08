package org.apache.cassandra.db;

import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class RowMutationCompletion implements ICompletable
{
    private final Message message;
    private final String id;
    private final RowMutation rm;
    
    private long startTime;

    private static Logger logger = LoggerFactory.getLogger(RowMutationCompletion.class);
    
    public RowMutationCompletion(Message message, String id, RowMutation rm)
    {
    	this.startTime = System.currentTimeMillis();
    	
        this.message = message;
        this.id = id;
        this.rm = rm;
    }

    // Complete the blocked RowMutation
    @Override
    public void complete()
    {
    	long endTime = System.currentTimeMillis();
    	long deltaTime = endTime - this.startTime;
    	logger.debug("~~~~ [DEEGAN] Completion Complete (" + deltaTime + "ms )" );
        RowMutationVerbHandler.instance().applyAndRespond(message, id, rm);
    }

}
