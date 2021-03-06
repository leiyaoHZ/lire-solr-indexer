package net.semanticmetadata.lire.solr;

import org.apache.lucene.document.*;
import net.semanticmetadata.lire.imageanalysis.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

//import javax.swing.ProgressMonitor;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.imageanalysis.bovw.LocalFeatureHistogramBuilder;
import net.semanticmetadata.lire.imageanalysis.bovw.SurfFeatureHistogramBuilder;
import net.semanticmetadata.lire.impl.ChainedDocumentBuilder;
import net.semanticmetadata.lire.impl.SurfDocumentBuilder;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;

public class Main {

    private static String indexDir = "index";
    private static String propFileLoc = "./config.properties";
    private static String clusterLoc = "./clusters-surf.dat";
    
	public static final void main(String[] args) {

		if (args.length >= 4 && "index".equals(args[0])) {
			try {
                boolean createVisualWords = Boolean.parseBoolean(args[2]);
                boolean createHistogram = Boolean.parseBoolean(args[3]);
                if (args.length>=5) indexDir = args[4];
                if (args.length>=6) propFileLoc = args[5];
                if (args.length>=7) clusterLoc = args[6];
                createIndex(args[1], createVisualWords,createHistogram);
            } catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else if (args.length >= 4 && "import".equals(args[0])) {
			try {
                boolean keepSurfLocalFeatures = Boolean.parseBoolean(args[1]);
                int start = Integer.parseInt(args[2]);
                int end = Integer.parseInt(args[3]);
                if (args.length>=5) indexDir = args[4];
                if (args.length>=6) propFileLoc = args[5];
                if (args.length>=7) clusterLoc = args[6];
                boolean copyClusterFile = true; 
                if (args.length>=8) copyClusterFile = Boolean.parseBoolean(args[7]);
    			importIndex(keepSurfLocalFeatures, start, end, copyClusterFile);
			} catch (IOException e) {
                e.printStackTrace();
				System.exit(1);
			} catch (SolrServerException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else if (args.length >= 4 && "indexVisualWords".equals(args[0])) {
            try{
                boolean createForMissing = Boolean.parseBoolean(args[1]);
                int start = Integer.parseInt(args[2]);
                int end = Integer.parseInt(args[3]);
                if (args.length>=5) indexDir = args[4];
                if (args.length>=6) propFileLoc = args[5];
                if (args.length>=7) clusterLoc = args[6];
                indexVisualWords(createForMissing, start, end);
            } catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			} catch (SolrServerException e) {
				e.printStackTrace();
				System.exit(1);
			}
        }else if (args.length >= 1 && "visualwords".equals(args[0])) {
			try {
                if (args.length>=2) indexDir = args[1];
                if (args.length>=3) propFileLoc = args[2];
                if (args.length>=4) clusterLoc = args[3];
				visualWords();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else if (args.length >= 3 && "readIndex".equals(args[0])) {
            try {
                int start = Integer.parseInt(args[1]);
                int end = Integer.parseInt(args[2]);
                if (args.length>=4) indexDir = args[3];
                if (args.length>=5) propFileLoc = args[4];
				readIndex(start,end);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
        } else if (args.length >= 2 && "readDoc".equals(args[0])) {
            try {
                String id = args[1];
                if (args.length>=3) indexDir = args[2];
                if (args.length>=4) propFileLoc = args[3];
                readDoc(id);
            } catch (IOException e) {
                e.printStackTrace();
				System.exit(1);
            }
        }
        else {
			printHelp();
		}
	}
    
    private static void readDoc(String id) throws IOException {
        IndexReader ir = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
        for (int i=0; i<ir.maxDoc(); i++)
        {
            Document doc = ir.document(i);
            String did = doc.getField(DocumentBuilder.FIELD_NAME_IDENTIFIER).stringValue();
            if(did.equals(id)) {
                if(doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS) != null)
                {
                    System.out.println("doc "+i+" - id "+id);
                    System.out.println("su_ha: "+doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS).stringValue());
                }
                else{
                    System.out.println(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS+": field not exist.");
                }
            }
            if (i%10000==0) {
                System.out.println("read "+Math.floor(1.0*i/ir.maxDoc()*100)+"%...");
            }
        }
		
    }
    
	private static void createIndex(String imagesFile, boolean createVisualWords, boolean createHistogram) throws FileNotFoundException, IOException {
		int numberOfThreads = Integer.parseInt(getProperties().getProperty("numberOfThreads"));
		ParallelIndexer indexer = new ParallelIndexer(numberOfThreads, indexDir, new File(imagesFile)) {
			@Override
			public void addBuilders(ChainedDocumentBuilder builder) {
				builder.addBuilder(new SurfDocumentBuilder());
				builder.addBuilder(new GenericDocumentBuilder(ColorLayout.class, DocumentBuilder.FIELD_NAME_COLORLAYOUT, true));
				builder.addBuilder(new GenericDocumentBuilder(EdgeHistogram.class, DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM, true));
				builder.addBuilder(new GenericDocumentBuilder(PHOG.class, DocumentBuilder.FIELD_NAME_PHOG, true));
//				builder.addBuilder(new GenericDocumentBuilder(OpponentHistogram.class, DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM, true));
				builder.addBuilder(new GenericDocumentBuilder(JCD.class, DocumentBuilder.FIELD_NAME_JCD, true));
			}
		};
		indexer.run();

		System.out.println("Indexing finished");
        
        LocalFeatureHistogramBuilder.DELETE_LOCAL_FEATURES = false;
        if (createVisualWords){
            IndexReader ir = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
            int numDocsForVocabulary = Integer.parseInt(getProperties().getProperty("numDocsForVocabulary"));
            int numClusters = Integer.parseInt(getProperties().getProperty("numClusters"));
            SurfFeatureHistogramBuilder sh = new SurfFeatureHistogramBuilder(ir, numDocsForVocabulary, numClusters, clusterLoc);
            //sh.setProgressMonitor(new ProgressMonitor(null, "", "", 0, 100));
            if (createHistogram){
                System.out.println("Creating visual words and histograms...");
                sh.index();
                System.out.println("Creating visual words and histograms finished.");
            }
            else{
                System.out.println("Creating visual words...");
                sh.createVisualWords();
                System.out.println("Creating visual words finished.");
            }
        }
        else if(createHistogram){
            IndexReader ir = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
            int numDocsForVocabulary = Integer.parseInt(getProperties().getProperty("numDocsForVocabulary"));
            int numClusters = Integer.parseInt(getProperties().getProperty("numClusters"));
            SurfFeatureHistogramBuilder sh = new SurfFeatureHistogramBuilder(ir, numDocsForVocabulary, numClusters, clusterLoc);
            System.out.println("Creating histograms for all...");
            int indexBatchSize = Integer.parseInt(getProperties().getProperty("indexBatchSize"));
            int batchNum = (int)Math.ceil(1.0*ir.maxDoc()/indexBatchSize);
            System.out.println(batchNum+" batches in total...");
            boolean createForMissing = false;
            for (int i=0; i<batchNum-1; i++){
                System.out.println("Batch "+i+" - creating histograms for docs "+i*indexBatchSize+"..."+((i+1)*indexBatchSize-1));
                sh.batchIndexUsingExistingClusters(i*indexBatchSize,(i+1)*indexBatchSize,createForMissing);
            }
            if(batchNum>0) {
                System.out.println("Batch "+(batchNum-1)+" - creating missing histograms for docs "+(batchNum-1)*indexBatchSize+"..."+(ir.maxDoc()-1));
                sh.batchIndexUsingExistingClusters((batchNum-1)*indexBatchSize,ir.maxDoc(), createForMissing);
            }
            System.out.println("Creating histograms finished.");
        }
		
		System.out.println("Now you can import data to solr by typing.");
		System.out.println("java -jar indexer.jar import missingVisualWords keepSurfLocalFeatures");
	}
    
    private static void indexVisualWords(boolean createForMissing, int start, int end) throws IOException, SolrServerException {
        LocalFeatureHistogramBuilder.DELETE_LOCAL_FEATURES = false;
        IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
        Properties prop = getProperties();
        int numDocsForVocabulary = Integer.parseInt(prop.getProperty("numDocsForVocabulary"));
        int numClusters = Integer.parseInt(prop.getProperty("numClusters"));
        SurfFeatureHistogramBuilder sh = new SurfFeatureHistogramBuilder(reader, numDocsForVocabulary, numClusters, clusterLoc);
        System.out.println("Creating missing histograms only...");
        sh.batchIndexUsingExistingClusters(start,end, createForMissing);
        System.out.println("Creating histograms finished.");
    }
    
    private static void readIndex(int start, int end) throws IOException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
        
        if (start<0 && end<0){
            System.out.println("Total # documents in index "+reader.maxDoc());
            return;
        }
        
        start = Math.max(0,start);
        end = Math.min(reader.maxDoc(), end);
        
        Bits liveDocs = MultiFields.getLiveDocs(reader);
		for (int i = start; i < end; ++i) {
            if (reader.hasDeletions() && !liveDocs.get(i)){
                System.out.println("doc "+i+" deleted.");
                continue;
            }
			Document doc = reader.document(i);
            System.out.println("reading doc "+i+": "+doc.getField(DocumentBuilder.FIELD_NAME_IDENTIFIER).stringValue());
            if(doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS) != null)
            {
                System.out.println("su_ha: "+doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS).stringValue());
            }
            else
            {
                 System.out.println(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS+": field not exist.");
            }
        }
    }

	private static void importIndex(boolean keepSurfLocalFeatures, int start, int end, boolean copyClusterFile) throws IOException, SolrServerException {
		Properties prop = getProperties();
		String solrCoreData = prop.getProperty("solrCoreData");
        if (copyClusterFile)
        {
            System.out.println("Copying clusters-surf.dat to " + solrCoreData);
            FileUtils.copyFile(new File(clusterLoc), new File(solrCoreData + "/clusters-surf.dat"));
        }

		String url = prop.getProperty("solrCoreUrl");
		System.out.println("Load data to: " + url);
		SolrServer server = new HttpSolrServer(url);
        
		Collection<SolrInputDocument> buffer = new ArrayList<>(30);
        IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
        Bits liveDocs = MultiFields.getLiveDocs(reader);
        
        if (start<0 && end<0){
            start = 0;
            end = reader.maxDoc();
        }
        else{
            start = Math.max(0, start);
            end = Math.min(reader.maxDoc(),end);
        }
        
		for (int i = start; i < end; ++i) {
            if (reader.hasDeletions() && !liveDocs.get(i)) continue;
			Document doc = reader.document(i);
            System.out.println("processing doc "+i+": "+doc.getField(DocumentBuilder.FIELD_NAME_IDENTIFIER).stringValue());
			SolrInputDocument inputDoc = new SolrInputDocument();
			// ID
			inputDoc.addField("id", doc.getField(DocumentBuilder.FIELD_NAME_IDENTIFIER).stringValue());

			// ColorLayout
			BytesRef clHiBin = doc.getField(DocumentBuilder.FIELD_NAME_COLORLAYOUT).binaryValue();
			inputDoc.addField("cl_hi", ByteBuffer.wrap(clHiBin.bytes, clHiBin.offset, clHiBin.length));
			inputDoc.addField("cl_ha", doc.getField(DocumentBuilder.FIELD_NAME_COLORLAYOUT + GenericDocumentBuilder.HASH_FIELD_SUFFIX).stringValue());


			BytesRef ehHiBin = doc.getField(DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM).binaryValue();
			inputDoc.addField("eh_hi", ByteBuffer.wrap(ehHiBin.bytes, ehHiBin.offset, ehHiBin.length));
			inputDoc.addField("eh_ha", doc.getField(DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM + GenericDocumentBuilder.HASH_FIELD_SUFFIX).stringValue());

			BytesRef phHiBin = doc.getField(DocumentBuilder.FIELD_NAME_PHOG).binaryValue();
			inputDoc.addField("ph_hi", ByteBuffer.wrap(phHiBin.bytes, phHiBin.offset, phHiBin.length));
			inputDoc.addField("ph_ha", doc.getField(DocumentBuilder.FIELD_NAME_PHOG + GenericDocumentBuilder.HASH_FIELD_SUFFIX).stringValue());

//			BytesRef ohHiBin = doc.getField(DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM).binaryValue();
//			inputDoc.addField("oh_hi", ByteBuffer.wrap(ohHiBin.bytes, ohHiBin.offset, ohHiBin.length));
//			inputDoc.addField("oh_ha", doc.getField(DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM + GenericDocumentBuilder.HASH_FIELD_SUFFIX).stringValue());

			BytesRef jcHiBin = doc.getField(DocumentBuilder.FIELD_NAME_JCD).binaryValue();
			inputDoc.addField("jc_hi", ByteBuffer.wrap(jcHiBin.bytes, jcHiBin.offset, jcHiBin.length));
			inputDoc.addField("jc_ha", doc.getField(DocumentBuilder.FIELD_NAME_JCD + GenericDocumentBuilder.HASH_FIELD_SUFFIX).stringValue());


			// SURF
            if (keepSurfLocalFeatures)
            {
                IndexableField[] features = doc.getFields(DocumentBuilder.FIELD_NAME_SURF);
                ArrayList<ByteBuffer> values = new ArrayList<ByteBuffer>(features.length);
                for (IndexableField feature : features) {
                    BytesRef featureBin = feature.binaryValue();
                    // inputDoc.setField("su_hi", ByteBuffer.wrap(featureBin.bytes, featureBin.offset, featureBin.length));
                    values.add(ByteBuffer.wrap(featureBin.bytes, featureBin.offset, featureBin.length));
                }
                inputDoc.addField("su_hi", values);
            }
            // SURF visual words
            if(doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS) != null)
            {
                inputDoc.addField("su_ha", doc.getField(DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS).stringValue());
            }

			buffer.add(inputDoc);

			if (buffer.size() >= 1) {
				// Flush buffer
				server.add(buffer);
				buffer.clear();
			}
            
            System.out.println("Done processing doc "+i);
		}

		if (buffer.size() > 0) {
			server.add(buffer);
			buffer.clear();
		}

		try {
			server.commit();
			server.shutdown();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

	private static void visualWords() throws IOException {
        System.out.println("Deprecated. Generate a list of random files and use index to create visual words.");
        System.exit(0);
		Properties prop = getProperties();
		IndexReader ir = DirectoryReader.open(FSDirectory.open(new File(indexDir)));
		LocalFeatureHistogramBuilder.DELETE_LOCAL_FEATURES = false;
		int numDocsForVocabulary = Integer.parseInt(prop.getProperty("numDocsForVocabulary"));
		int numClusters = Integer.parseInt(prop.getProperty("numClusters"));
		SurfFeatureHistogramBuilder sh = new SurfFeatureHistogramBuilder(ir, numDocsForVocabulary, numClusters, clusterLoc);
		//sh.setProgressMonitor(new ProgressMonitor(null, "", "", 0, 100));
		//sh.index();
        sh.createVisualWords();
	}

	private static Properties getProperties() {
		Properties prop = new Properties();

		try {
			prop.load(new FileInputStream(propFileLoc));
		} catch (IOException e) {
			System.out.println("Cannot read config.properties file.");
		}
		return prop;
	}

	private static void printHelp() {
		System.out.println("USAGE:");
		System.out.println("\t index file createVisualWords createHistogram indexDir\n\t\t file - File contains paths to the images, which will be indexed. \n\t\t createVisualWords - boolean value, whether creating visual words. \n\t\t createHistogram - boolean value, whether creating histogram.");
		System.out.println("\t import keepSurfLocalFeatures start end indexDir\n\t\t It sends data from index to solr server specific in the config.properties file. \n\t\t keepSurfLocalFeatures - boolean, if true, keep SURF features in index.");
		System.out.println("\t visualwords indexDir\n\t\t It creates data for visual words technique. You can execute this step again if you want to create visual words with other parameters specific in config.properties file.");
        System.out.println("\t indexVisualWords createForMissing start end indexDir\n\t\t It creates data for visual words technique. ");
        System.out.println("\t readIndex start end indexDir\n\t\t print out docs in index.");
        System.out.println("\t readDoc id indexDir\n\t\t print out a doc in index.");
	}
}
