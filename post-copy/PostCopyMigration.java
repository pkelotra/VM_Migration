// PostCopyMigration.java

import java.util.*;
import java.util.concurrent.*;

/**
 * Manages the state and process of a Pure Post-Copy Live Migration with Demand Paging ONLY.        resumeTime = System.currentTimeMillis() - resumeStartTime;
        System.out.println();
        System.out.println("> Post-Copy Migration Resume Phase completed in " + resumeTime + " ms"); No Active Push - pages are transferred only when the VM actually needs them (page faults).
 * Uses int arrays for memory representation instead of MemoryPage objects.
 */
class PostCopyMigration {
    // Memory Page Configuration
    public static final int PAGE_SIZE_KB = 4; // Standard 4KB page size
    public static final int PAGE_SIZE_BYTES = PAGE_SIZE_KB * 1024; // 4096 bytes
    
    // Network configuration
    private final double linkSpeedMbps; // Link speed in Mbps
    private final long pageTransferTimeMs; // Calculated transfer time per page in milliseconds
    
    // Memory structures - using int arrays for memory representation
    private final int[] sourceMemory;     // Source VM memory (0=free, 1=used, 2=transferred)
    private final int[] targetMemory;     // Target VM memory (0=not_present, 1=present)
    
    // Queue for page fault IDs from Target -> Source (Demand Paging)
    private final BlockingQueue<Integer> faultQueue = new LinkedBlockingQueue<>();
    private final Set<Integer> transferredPages = new HashSet<>();
    
    private final int TOTAL_PAGES;
    private final int NON_PAGEABLE_PAGES; // Dynamic based on VM size

    // Timing Metrics for Post-Copy Migration Analysis
    private long migrationStartTime;
    private long preparationStartTime;
    private long downtimeStartTime;
    private long downtimeEndTime;
    private long resumeStartTime;
    private long migrationEndTime;
    
    // Performance Metrics
    private long totalPagesTransferred = 0;
    private long pageFaults = 0;
    private long memoryAccesses = 0;
    
    // Thread completion flags
    private volatile boolean migrationComplete = false;
    private volatile boolean vmWorkloadComplete = false;
    private volatile boolean demandPagingComplete = false;

    // Simulation Setup
    public PostCopyMigration(int totalPages, double freePageRatio, double linkSpeedMbps) {
        this.TOTAL_PAGES = totalPages;
        this.linkSpeedMbps = linkSpeedMbps;
        
        // Calculate page transfer time based on link speed
        // Formula: (Page size in bits) / (Link speed in bps) * 1000 (to convert to ms)
        double pageSizeBits = PAGE_SIZE_BYTES * 8.0; // Convert bytes to bits
        double linkSpeedBps = linkSpeedMbps * 1_000_000.0; // Convert Mbps to bps
        this.pageTransferTimeMs = Math.max(1, (long)(pageSizeBits / linkSpeedBps * 1000));
        
        // Calculate NON_PAGEABLE_PAGES as 0.5% of total pages, minimum 5, maximum 50
        this.NON_PAGEABLE_PAGES = Math.max(5, Math.min(50, (int)(totalPages * 0.005)));
        
        // Initialize memory arrays
        this.sourceMemory = new int[totalPages];     // 0=free, 1=used, 2=transferred
        this.targetMemory = new int[totalPages];     // 0=not_present, 1=present
        
        initializeMemory(freePageRatio);
    }

    private void initializeMemory(double freePageRatio) {
        Random random = new Random();
        
        // Initialize source memory array
        // 0 = free page, 1 = used page, 2 = transferred page
        for (int i = 0; i < TOTAL_PAGES; i++) {
            sourceMemory[i] = random.nextDouble() < freePageRatio ? 0 : 1;  // 0=free, 1=used
        }
        
        // Initialize target memory array (all pages initially not present)
        Arrays.fill(targetMemory, 0);  // 0=not_present, 1=present
        
        System.out.println("Initialized " + TOTAL_PAGES + " pages in source memory");
    }

    public void start() throws InterruptedException {
        migrationStartTime = System.currentTimeMillis();
        
        printHeader();
        printVMConfiguration();
        
        // Phase 1: Preparation (Live)
        performPreparation();
        
        // Phase 2: Downtime (VM Suspended)
        performDowntime();
        
        // Phase 3: Resume (Demand Paging Only)
        performResume();
        
        migrationEndTime = System.currentTimeMillis();
        printResults();
    }

    private void printHeader() {
        System.out.println("==================================================================");
        System.out.println("    PURE POST-COPY LIVE VM MIGRATION SIMULATION");
        System.out.println("    (DEMAND PAGING ONLY - NO ACTIVE PUSH)");
        System.out.println("==================================================================");
    }

    private void printVMConfiguration() {
        double vmSizeMB = (TOTAL_PAGES * PAGE_SIZE_KB) / 1024.0;
        System.out.println("VM Configuration:");
        System.out.println("  - Total Pages: " + TOTAL_PAGES + " pages");
        System.out.println("  - Page Size: " + PAGE_SIZE_KB + " KB (" + PAGE_SIZE_BYTES + " bytes)");
        System.out.println("  - Total VM Memory: " + String.format("%.1f", vmSizeMB) + " MB");
        System.out.println("  - Non-pageable pages: " + NON_PAGEABLE_PAGES + " (critical system pages)");
        System.out.println("  - Link Speed: " + linkSpeedMbps + " Mbps");
        System.out.println("  - Page Transfer Time: " + pageTransferTimeMs + " ms per page");
        System.out.println();
    }

    private void performPreparation() {
        System.out.println("--- PHASE 1: PREPARATION (LIVE) ---");
        System.out.println("Initiating migration process...");
        
        preparationStartTime = System.currentTimeMillis();
        
        System.out.println("DSB (Dirty State Bitmap) Activated");
        
        // Count free pages in memory array
        int reclaimedPages = 0;
        for (int i = 0; i < TOTAL_PAGES; i++) {
            if (sourceMemory[i] == 0) {  // free page
                reclaimedPages++;
            }
        }
        
        long preparationTime = System.currentTimeMillis() - preparationStartTime;
        
        System.out.println("   > Reclaimed " + reclaimedPages + " free pages to reduce memory footprint");
        System.out.println("   > Preparation Time: " + preparationTime + " ms");
    }

    private void performDowntime() throws InterruptedException {
        System.out.println();
        System.out.println("--- PHASE 2: DOWNTIME (VM SUSPENDED) ---");
        System.out.println("VM Execution STOPPED - Suspending VM......");
        
        downtimeStartTime = System.currentTimeMillis();
        
        System.out.println("Transferring critical state...");
        
        // Simulate transferring critical pages (non-pageable)
        for (int i = 0; i < NON_PAGEABLE_PAGES; i++) {
            System.out.print(".");
            if (i % 10 == 9) System.out.println();
            Thread.sleep(pageTransferTimeMs); 
            
            // Mark page as transferred in both source and target
            sourceMemory[i] = 2;  // 2 = transferred
            targetMemory[i] = 1;  // 1 = present
            transferredPages.add(i);
            totalPagesTransferred++;
        }
        System.out.println();
        
        downtimeEndTime = System.currentTimeMillis();
        
        System.out.println("VM RESUMED on Target Host");
        System.out.println("   > CPU State + " + NON_PAGEABLE_PAGES + " critical pages transferred");
        System.out.println("   > Downtime Duration: " + (downtimeEndTime - downtimeStartTime) + " ms");
        System.out.println("   > VM is now running on target - pages will be pulled ON DEMAND ONLY");
    }

    private void performResume() throws InterruptedException {
        System.out.println();
        System.out.println("--- PHASE 3: RESUME TIME (DEMAND PAGING ONLY) ---");
        
        resumeStartTime = System.currentTimeMillis();
        
        // Start demand paging service thread
        Thread demandPagingThread = new Thread(this::demandPagingService);
        demandPagingThread.setName("DemandPagingService");
        demandPagingThread.start();
        
        // Start VM execution thread
        Thread vmExecutionThread = new Thread(this::vmExecutionService);
        vmExecutionThread.setName("VMExecution");
        vmExecutionThread.start();
        
        // Wait for VM workload to complete naturally
        while (!vmWorkloadComplete && !Thread.currentThread().isInterrupted()) {
            Thread.sleep(100);
        }
        
        // Give some time for final page transfers to complete
        Thread.sleep(500);
        
        // Stop simulation
        migrationComplete = true;
        demandPagingThread.interrupt();
        vmExecutionThread.interrupt();
        
        demandPagingThread.join(2000);
        vmExecutionThread.join(2000);
        
        long resumeTime = System.currentTimeMillis() - resumeStartTime;
        System.out.println();
        System.out.println("Post-Copy Migration Resume Phase completed in " + resumeTime + " ms");
    }

    private void demandPagingService() {
        System.out.println("Demand Paging Service (Source): Waiting for page fault requests...");
        
        while (!Thread.currentThread().isInterrupted() && !migrationComplete) {
            try {
                Integer pageId = faultQueue.poll(1, TimeUnit.SECONDS);
                if (pageId != null) {
                    // Check if page exists and is not already transferred
                    if (pageId < TOTAL_PAGES && sourceMemory[pageId] != 2) {
                        // Simulate page transfer with realistic timing
                        Thread.sleep(pageTransferTimeMs);
                        
                        // Mark as transferred in source and present in target
                        sourceMemory[pageId] = 2;  // 2 = transferred
                        targetMemory[pageId] = 1;  // 1 = present
                        transferredPages.add(pageId);
                        totalPagesTransferred++;
                        
                        System.out.println("   > Page " + pageId + " transferred on demand (Total: " + transferredPages.size() + " pages)");
                    }
                } else {
                    System.out.println("   Timeout waiting for page " + new Random().nextInt(TOTAL_PAGES));
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        
        demandPagingComplete = true;
        System.out.println("Demand Paging Service completed: " + (totalPagesTransferred - NON_PAGEABLE_PAGES) + " pages transferred on demand");
    }

    private void vmExecutionService() {
        System.out.println("VM Execution (Target): Running application with DEMAND PAGING");
        
        Random random = new Random();
        int accessCount = 0;
        // Scale accesses with VM size, but keep reasonable bounds
        int maxAccesses = Math.max(20, Math.min(100, TOTAL_PAGES / 5000));
        
        while (!Thread.currentThread().isInterrupted() && accessCount < maxAccesses) {
            try {
                // Simulate VM accessing a random page
                int pageId = random.nextInt(TOTAL_PAGES);
                memoryAccesses++;
                
                // Check if page is available in target memory
                if (targetMemory[pageId] == 0) {  // page not present
                    // Page fault - request page from source
                    faultQueue.offer(pageId);
                    pageFaults++;
                    accessCount++;
                    
                    if (accessCount % 10 == 0) {
                        double transferPercentage = (double) transferredPages.size() / TOTAL_PAGES * 100;
                        System.out.println("   Page Fault #" + pageFaults + " - Requesting page " + pageId + " (Transferred: " + String.format("%.1f", transferPercentage) + "%)");
                    }
                    
                    // Wait for page to be transferred
                    while (targetMemory[pageId] == 0 && !Thread.currentThread().isInterrupted()) {
                        Thread.sleep(10);
                    }
                }
                
                Thread.sleep(50);
                
            } catch (InterruptedException e) {
                break;
            }
        }
        
        vmWorkloadComplete = true;
        System.out.println("VM workload completed after " + accessCount + " page faults / " + memoryAccesses + " total accesses");
    }

    private void printResults() {
        System.out.println();
        System.out.println("======================================================================");
        System.out.println("             PURE POST-COPY MIGRATION COMPLETED");
        System.out.println("======================================================================");
        System.out.println();
        
        long totalMigrationTime = migrationEndTime - migrationStartTime;
        long preparationTime = downtimeStartTime - preparationStartTime;
        long downtimeLength = downtimeEndTime - downtimeStartTime;
        long resumeTime = migrationEndTime - resumeStartTime;
        
        double vmSizeMB = (TOTAL_PAGES * PAGE_SIZE_KB) / 1024.0;
        double transferredMB = (totalPagesTransferred * PAGE_SIZE_KB) / 1024.0;
        double savedMB = ((TOTAL_PAGES - totalPagesTransferred) * PAGE_SIZE_KB) / 1024.0;
        double transferPercentage = (double) totalPagesTransferred / TOTAL_PAGES * 100;
        double bandwidthSavings = (double) (TOTAL_PAGES - totalPagesTransferred) / TOTAL_PAGES * 100;
        double faultRate = memoryAccesses > 0 ? (double) pageFaults / memoryAccesses * 100 : 0;
        
        System.out.println("DEMAND PAGING POST-COPY MIGRATION METRICS:");
        System.out.println("   - Preparation Time: " + preparationTime + " ms");
        System.out.println("   - Downtime (VM Suspended): " + downtimeLength + " ms");
        System.out.println("   - Resume Time (VM Running): " + resumeTime + " ms");
        System.out.println("   - TOTAL MIGRATION TIME: " + totalMigrationTime + " ms");
        System.out.println();
        
        System.out.println("MEMORY TRANSFER STATISTICS:");
        System.out.println("   - VM Size: " + TOTAL_PAGES + " pages (" + String.format("%.1f", vmSizeMB) + " MB)");
        System.out.println("   - Page Size: " + PAGE_SIZE_KB + " KB per page");
        System.out.println("   - Non-pageable Pages (Downtime): " + NON_PAGEABLE_PAGES + " pages (" + (NON_PAGEABLE_PAGES * PAGE_SIZE_KB) + " KB)");
        System.out.println("   - Pages Transferred on Demand: " + (totalPagesTransferred - NON_PAGEABLE_PAGES) + " pages (" + ((totalPagesTransferred - NON_PAGEABLE_PAGES) * PAGE_SIZE_KB) + " KB)");
        System.out.println("   - Total Pages Transferred: " + totalPagesTransferred + " pages (" + (int)(transferredMB * 1024) + " KB)");
        System.out.println("   - Total Data Transferred: " + String.format("%.2f", transferredMB) + " MB");
        System.out.println("   - Pages NOT Transferred: " + (TOTAL_PAGES - totalPagesTransferred) + " pages (" + String.format("%.2f", savedMB) + " MB saved!)");
        System.out.println("   - Pages Transferred: " + String.format("%.3f", transferPercentage) + "% of VM");
        System.out.println("   - Bandwidth Savings: " + String.format("%.3f", bandwidthSavings) + "% saved");
        System.out.println("   - Post-Copy Efficiency: " + (bandwidthSavings > 90 ? "EXCELLENT" : bandwidthSavings > 80 ? "GOOD" : "FAIR"));
        System.out.println("   - Page Faults: " + pageFaults);
        System.out.println("   - Total Memory Accesses: " + memoryAccesses);
        System.out.println("   - Fault Rate: " + String.format("%.2f", faultRate) + "%");
        System.out.println();
        
        System.out.println("APPLICATION IMPACT ANALYSIS:");
        System.out.println("   - VM Downtime: " + downtimeLength + " ms (Service Interruption)");
        System.out.println("   - Performance Degradation: " + resumeTime + " ms (Due to page faults)");
        System.out.println("   - Downtime Percentage: " + String.format("%.2f", (double)downtimeLength / totalMigrationTime * 100) + "%");
        System.out.println("   - Source VM Resources: FREED");
        System.out.println();
        
        System.out.println("POST-COPY MIGRATION SUMMARY:");
        System.out.println("   - Only " + totalPagesTransferred + "/" + TOTAL_PAGES + " pages were actually needed and transferred");
        System.out.println("   - " + (TOTAL_PAGES - totalPagesTransferred) + " pages were never accessed (" + String.format("%.2f", savedMB) + " MB bandwidth saved!)");
        System.out.println("   - Migration completed in " + String.format("%.2f", totalMigrationTime / 1000.0) + " seconds");
        System.out.println("   - Average demand transfer rate: " + (totalPagesTransferred > 0 ? (totalPagesTransferred * 1000 / resumeTime) : 0) + " pages/second (" + String.format("%.2f", transferredMB * 1000 / resumeTime) + " MB/s)");
        System.out.println();
        System.out.println("Pure Post-Copy Live Migration with Demand Paging completed successfully!");
        System.out.println("Key benefit: Only transferred pages that were actually needed by the VM!");
    }
}