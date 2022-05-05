package com.company.dataStructures.KMeans;

import com.company.TFIDF.SortByTFIDF_Asgnt1;
import com.company.TFIDF.TFIDF_Asgnt1;
import com.company.TFIDF.Term;
import com.company.dataStructures.MyHashMap;
import com.company.dataStructures.bTree.MyBTree;

import java.io.*;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class KMeans {


    public static Map<String, ArrayList<String>> fit(MyBTree records, int k, int maxIterations, ArrayList<String> businessNames) throws IOException {
        List<MyHashMap> centroids = records.getRandomBusinesses(k);//randomCentroids(records, k);
        Map<MyHashMap, MyBTree> clusters = new HashMap<>();
        Map<MyHashMap, MyBTree> lastState = new HashMap<>();
        ArrayList<String> fileNames = new ArrayList<>();

        // iterate for a pre-defined number of times
        for (int i = 0; i < maxIterations; i++) {

            boolean isLastIteration = i == maxIterations - 1;

            // in each iteration we should find the nearest centroid for each record
            for (int j = 0; j < records.getSize(); j++) {
                System.out.println("iteration: " + j + "||||| businessName: " + businessNames.get(j));
                MyHashMap currentBusiness = records.search(businessNames.get(j));
//                if (currentBusiness == null){
//                    continue;
//                }
//                System.out.printf("Current business: %s\nCentroids: %s ||| %s ||| %s ||| %s ||| %s\n",
                        currentBusiness.getBusinessName();
//                        centroids.get(0).getBusinessName(),
//                        centroids.get(1).getBusinessName(),
//                        centroids.get(2).getBusinessName(),
//                        centroids.get(3).getBusinessName(),
//                        centroids.get(4).getBusinessName()
//                        );
                MyHashMap centroid = nearestCentroid(currentBusiness, centroids);
                //store business names for centroids in files for later use
                String fileName = System.getProperty("user.dir") +"/9_"+centroid.getBusinessName() + ".txt";
                File file = new File(fileName);
                if (!fileNames.contains(fileName))
                    fileNames.add(fileName);
                FileWriter fileWriter = new FileWriter(file, true);
                fileWriter.write(businessNames.get(j)+"\n");
                fileWriter.close();

                int x = j;
                clusters.compute(centroid, (key, bTree) -> { try {
                    if (bTree == null)
                        bTree = new MyBTree(36);
                    bTree.insert(records.search(businessNames.get(x)));

                } catch (IOException e) {e.printStackTrace();}
                return bTree;
                });
            }

            // if the assignments do not change, then the algorithm terminates
            boolean shouldTerminate = isLastIteration || clusters.equals(lastState);
            System.out.println("TERMINATE CHECK");
            lastState = clusters;
            if (shouldTerminate) {
                System.out.println("TERMINATING");
                removeFiles(fileNames);
                break;
            }

            // at the end of each iteration we should relocate the centroids
            System.out.println("RELOCATING CENTROIDS");
            centroids = relocateCentroids(clusters);
            removeFiles(fileNames);
            fileNames.clear();
            clusters = new HashMap<>();
        }

        Map<String, ArrayList<String>> finalClusters = new HashMap<>();
        ArrayList<String> value;
        String key;
        for(Map.Entry<MyHashMap, MyBTree> entry: lastState.entrySet()){
            key = entry.getKey().getBusinessName();
            value = entry.getValue().businessNameArrayListRep();
            finalClusters.put(key, value);
        }

        return finalClusters;
    }

    private static MyHashMap average(MyHashMap centroid, MyBTree records) throws IOException {
        if (records == null || records.getSize() == 0) {
            return centroid;
        }
        double[] tfidfs = new double[records.getSize()];
        ArrayList<String> businessNames = new ArrayList<>();
        String fileName = System.getProperty("user.dir") +"/9_"+centroid.getBusinessName() + ".txt";
        File file = new File(fileName);
        FileReader fileReader = new FileReader(file);
        Scanner scanner = new Scanner(fileReader);
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNext())
            businessNames.add(scanner.nextLine());
        fileReader.close();

//        businessNames = sb.toString().split("\n");

        for(int i = 0; i < records.getSize(); i++) {
            MyHashMap currentHashMap = records.search(businessNames.get(i));
            if (currentHashMap == null){
                continue;
            }
//            System.out.println("AVERAGE FUNCTION CURRENT HASHMAP: |" + centroid.getBusinessName() + "| "+ currentHashMap.getBusinessName() + "  " + records.getSize() + " of " + i);
            currentHashMap.setTfidf(0);
            for (LinkedList<Term> ll : centroid.map) {
                if (ll == null)
                    continue;
                for (Term key : ll) {
                    currentHashMap.addToTfidf(TFIDF_Asgnt2.CalculateTFIDF(records, currentHashMap, key.key, records.getSize(), businessNames));
                }
            }
            tfidfs[i] = currentHashMap.getTFIDF();
        }

        int meanIndex = findMeanIndex(tfidfs);
        MyHashMap myHashMap = records.search(businessNames.get(meanIndex));
        System.out.printf("NEW CENTROID: %s ---> %s\n",centroid.getBusinessName() , myHashMap.getBusinessName());
        return myHashMap;
    }

    private static List<MyHashMap> relocateCentroids(Map<MyHashMap, MyBTree> clusters) {
        System.out.print("---List of My OLD Centroids---: ");
        for(Map.Entry<MyHashMap, MyBTree> entry: clusters.entrySet()){
            System.out.print(entry.getKey().getBusinessName() + "|||");
        }
        System.out.println();
        List<MyHashMap> myHashMaps = clusters.entrySet().stream().map(e -> {
            try {
                return average(e.getKey(), e.getValue());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return null;
        }).collect(toList());
        System.out.printf("List of My new Centroids: %s ||| %s ||| %s ||| %s ||| %s\n",
                myHashMaps.get(0).getBusinessName(),
                myHashMaps.get(1).getBusinessName(),
                myHashMaps.get(2).getBusinessName(),
                myHashMaps.get(3).getBusinessName(),
                myHashMaps.get(4).getBusinessName());
        return myHashMaps;
    }

    public static int findMeanIndex(double[] tfidfs){
        double mean = 0;
        double smallest = tfidfs[0];
        int smallestIndex = 0;
        for (double value : tfidfs) {
            mean += value;
        }
        mean /= tfidfs.length;

        int i = 0;
        for (double tfidf : tfidfs) {
            if (Math.abs(tfidf - mean) < smallest) {
                smallest = Math.abs(tfidf - mean);
                smallestIndex = i;
            }
            i++;
        }
        System.out.print("TFIDFS: ");
        for (int j = 0; j < tfidfs.length; j++)
            System.out.print(tfidfs[j] + ", ");
        System.out.println("\nSmallest Index: " + smallestIndex);
        return smallestIndex;
    }

    private static MyHashMap nearestCentroid(MyHashMap record, List<MyHashMap> centroids) {
        for(MyHashMap maps: centroids) {
            maps.setTfidf(0);
            //Layer 2
            for (LinkedList<Term> ll : record.map) {
                if (ll == null)
                    continue;
                for (Term key : ll) {
                    maps.addToTfidf(TFIDF_Asgnt1.CalculateTFIDF(allToOneHashmap(centroids), maps, key.key, centroids.size()));
                }
            }
        }

        SortByTFIDF_Asgnt1 sortByTFIDF = new SortByTFIDF_Asgnt1();
        centroids.sort(sortByTFIDF);

        return centroids.get(0);
    }

    public static MyHashMap allToOneHashmap(List<MyHashMap> myHashMaps){
        //Layer 1
        MyHashMap allHashMaps = new MyHashMap();
        for(MyHashMap maps: myHashMaps) {
            //Layer 2
            for (LinkedList<Term> ll : maps.map) {
                if (ll == null)
                    continue;
                for (Term key : ll) {
                    allHashMaps.put(key.key);
                }
            }
        }
        return allHashMaps;
    }

    private static void removeFiles(ArrayList<String> fileNames){
        for (String fileName : fileNames) {
            File file = new File(fileName);
            System.out.println(file.exists()+ "|||||||||||||||||||||||||||||||"+fileName);
            if (!file.delete())
                System.out.println("-------------------------NOT DELETED------------------------------");
            else
                System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^deleted^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        }

    }

}
