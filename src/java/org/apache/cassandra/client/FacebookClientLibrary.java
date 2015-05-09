package org.apache.cassandra.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Dep;
import org.apache.cassandra.thrift.GetRangeSlicesResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.LamportClock;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FacebookClientLibrary {
	
	private static int sequenceNum = 0;
	private ClientLibrary lib;
	private Map<String, Integer> localServerIPAndPorts;
	public boolean useEiger = false;
	
	private abstract class FBContent {
		//Constants
		public static final String serialize_token = ":";
		public static final String key_token = "#";
		//Members
		public String writerId;
		public String content;
		protected String wallId;
		protected String key;
		private long timestamp;
		protected abstract String serialize();
		abstract void deserialize(String s);
		public FBContent(String wallId, String writerId, String content) {
			this.wallId = wallId;
			this.writerId = writerId;
			this.content = content;
			this.timestamp = System.currentTimeMillis();
			this.key = generateKey(this.wallId, this.writerId);
		}
		protected FBContent(String key, Column col) {
			this.timestamp = col.timestamp;
			this.key = key;
			this.wallId = new String(col.getName());
			this.deserialize(new String(col.getValue()));
		}
		
		public long getTimestamp(){
			return timestamp;
		}
		
		private String generateKey(String wallId, String writerId) {
			return (wallId + key_token + writerId + key_token + sequenceNum++);
		}

		protected Dep asDep() {
			return new Dep(toBytes(this.key), this.timestamp);
		}
	}
	
	public class FBPost extends FBContent {
		public static final String post_tag = "<post>";
		public List<FBComment> comments = new ArrayList<>();
		protected String serialize(){
			return post_tag + serialize_token + this.writerId + serialize_token + this.content;
		}
		void deserialize(String s) {
			String[] components = s.split(serialize_token);
			assert components.length == 3;
			assert components[0].equals(post_tag);
			this.writerId = components[1];
			this.content = components[2];
		}
		// This is the constructor for a new post
		public FBPost(String wallId, String writerId, String content) {
			super(wallId, writerId, content);
		}
		protected FBPost(String key, Column col){
			super(key, col);
		}
	}
	
	public class FBComment extends FBContent {
		public static final String comment_tag = "<comment>";
		private String parentCommentkey;
		protected String serialize(){
			return comment_tag + serialize_token + this.writerId + serialize_token + this.content + serialize_token + this.parentCommentkey;
		}
		void deserialize(String s) {
			String[] components = s.split(serialize_token);
			assert components.length == 4;
			assert components[0].equals(comment_tag);
			this.writerId = components[1];
			this.content = components[2];
			this.parentCommentkey = components[3];
		}
		// This is the constructor for a new comment
		public FBComment(String writerId, String content, FBPost parentPost) {
			super(parentPost.wallId, writerId, content);
			this.parentCommentkey = parentPost.key;
		}
		protected FBComment(String key, Column col){
			super(key, col);
		}
	}

	public FacebookClientLibrary(Map<String, Integer> localServerIPAndPorts, String keyspace, ConsistencyLevel consistencyLevel) {
		try{
			this.localServerIPAndPorts = localServerIPAndPorts;
			this.lib = new ClientLibrary(localServerIPAndPorts, keyspace, consistencyLevel);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public List<FBPost> getWallPosts(String wallId) {

//		String query = "SELECT " +  wallId + " FROM Facebook.Walls";
//		System.out.println(query);
		
		Entry<String, Integer> entry = this.localServerIPAndPorts.entrySet().iterator().next();
		TTransport tFramedTransport = new TFramedTransport(new TSocket(entry.getKey(), entry.getValue()));
        TProtocol binaryProtoOnFramed = new TBinaryProtocol(tFramedTransport);
        Cassandra.Client client = new Cassandra.Client(binaryProtoOnFramed);
        
        SlicePredicate predicate = new SlicePredicate();
		predicate.addToColumn_names(toBytes(wallId));
		
		KeyRange range = new KeyRange();
		range.count = 1000;
		range.setStart_key(toBytes(""));
		range.setEnd_key(toBytes(""));
		
        try {
			tFramedTransport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
        
        try {
        	List<FBPost> posts = new ArrayList<>();
			List<FBComment> comments = new ArrayList<>();
			
        	client.set_keyspace("Facebook", LamportClock.sendTimestamp());
        	GetRangeSlicesResult res = null;
        	List<KeySlice> result;
            if(this.useEiger) {
            	result = lib.get_range_slices(columnParent, predicate, range);
            }
            else{
            	res = client.get_range_slices(columnParent, predicate, range, ConsistencyLevel.LOCAL_QUORUM, LamportClock.sendTimestamp());
            	result = res.value;
            }
            for(KeySlice s: result){
            	String key = new String(s.getKey());
            	for(ColumnOrSuperColumn c: s.columns) {
            		Column postContent = c.getColumn();
            		if(postContent.getValue()[1] == 'p') {
    					FBPost post = new FBPost(key, postContent);
    					posts.add(post);
    				}else{
    					FBComment comment = new FBComment(key, postContent);
    					comments.add(comment);
    				}
            	}
            }
            
            for(FBComment comment: comments) {
				for(FBPost post: posts) {
					if(post.key.equals(comment.parentCommentkey)){
						post.comments.add(comment);
					}
				}
			}
            
			return posts;
		} catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	private static final ColumnParent columnParent = new ColumnParent("Walls");

	public void makePost(String wallId, String content, String writerId, FBPost previousPostOrNull) {
		FBPost post = new FBPost(wallId, writerId, content);
		Dep dep = previousPostOrNull != null ? previousPostOrNull.asDep() : null;
		try {
			if(this.useEiger) this.lib.insert(toBytes(post.key), columnParent, this.newColumn(wallId, post.serialize(), post.getTimestamp()));
			else this.lib.modifiedInsert(toBytes(post.key), columnParent, this.newColumn(wallId, post.serialize(), post.getTimestamp()), dep);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void makeComment(String writerId, String content, FBPost parentPost) {
		Dep dep;
		//We will make this comment depend on the most recent comment or the post if there are no comments
		if(parentPost.comments.isEmpty()) {
			dep = parentPost.asDep();
		}
		else{
			FBComment mostRecent = parentPost.comments.get(0);
			for(FBComment c: parentPost.comments) {
				if(c.getTimestamp() > mostRecent.getTimestamp()) mostRecent = c;
			}
			dep = mostRecent.asDep();
		}
		FBComment comment = new FBComment(writerId, content, parentPost);
		try {
			if(this.useEiger) this.lib.insert(toBytes(comment.key), columnParent, this.newColumn(comment.wallId, comment.serialize() ,comment.getTimestamp()));
			else this.lib.modifiedInsert(toBytes(comment.key), columnParent, this.newColumn(comment.wallId, comment.serialize() ,comment.getTimestamp()), dep);
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	private Column newColumn(String name, String value, long timestamp) {
        return new Column(ByteBufferUtil.bytes(name)).setValue(ByteBufferUtil.bytes(value)).setTimestamp(timestamp);
    }
	
	private ByteBuffer toBytes(String s) { 
		return ByteBufferUtil.bytes(s);
	}
}
