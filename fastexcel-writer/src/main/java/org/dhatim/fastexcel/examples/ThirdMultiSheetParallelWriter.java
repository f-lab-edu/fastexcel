package org.dhatim.fastexcel.examples;

import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 다중 시트 병렬 엑셀 쓰기 성능 비교 테스트 (CompletableFuture 및 큐 기반)
 * 
 * 이 테스트는 데이터 생성을 위한 생산자 CompletableFuture와
 * 데이터를 큐에서 가져와 시트에 쓰는 소비자 CompletableFuture들을 사용합니다.
 */
public class ThirdMultiSheetParallelWriter {

    // 테스트 설정
    private static final int COLUMN_COUNT = 70;
    private static final int ROW_COUNT = 1_000_000;
    private static final int SHEET_THRESHOLD = 1_000_000; // 시트 당 행 개수 제한
    private static final int[] THREAD_COUNTS = {3, 4}; // 테스트할 소비자 스레드 수
    private static final boolean[] USE_MULTI_SHEET = {false, true}; // 단일 시트 vs 다중 시트
    private static final String OUTPUT_DIR = "excel_test_results";
    private static final int QUEUE_THRESHOLD = 100_000; // 데이터 큐 최대 크기
    
    // 데이터 생성을 위한 변수
    private static final String[] STRING_VALUES = {
            "텍스트 데이터", "샘플 값", "테스트 문자열", "엑셀 테스트", "다중 스레드", 
            "성능 테스트", "대용량 데이터", "FastExcel", "Java 테스트", "병렬 처리"
    };

    // 큐를 통해 전달될 데이터 구조체
    private static class CellData {
        final int sheetIndex;
        final int rowInSheet;
        final int col;
        final int value;

        public CellData(int sheetIndex, int rowInSheet, int col, int value) {
            this.sheetIndex = sheetIndex;
            this.rowInSheet = rowInSheet;
            this.col = col;
            this.value = value;
        }
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println("다중 시트 병렬 엑셀 쓰기 성능 비교 테스트 (CompletableFuture + 큐 기반) 시작");
        createOutputDirectory();
        
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path resultsPath = Paths.get(OUTPUT_DIR, "multi_sheet_cf_queue_results_" + timestamp + ".txt");
        
        Files.write(resultsPath, "시트 방식,스레드 수,실행 시간(ms),메모리 사용량(MB)\n".getBytes());
        
        for (boolean multiSheet : USE_MULTI_SHEET) {
            String sheetMode = multiSheet ? "다중 시트" : "단일 시트";
            
            for (int threadCount : THREAD_COUNTS) {
                System.out.println("\n" + sheetMode + ", 소비자 스레드 수 " + threadCount + "로 테스트 시작");
                
                double avgTime = 0;
                double avgMemory = 0;
                
                for (int i = 0; i < 3; i++) { // 반복 테스트
                    System.out.println("  반복 #" + (i + 1));
                    TestResult result = runTest(multiSheet, threadCount, 
                            timestamp + "_" + (multiSheet ? "multi_cf_queue" : "single_cf_queue") + "_thread" + threadCount + "_run" + (i + 1));
                    avgTime += result.executionTimeMs;
                    avgMemory += result.memoryUsageMB;
                    
                    String resultLine = sheetMode + "," + threadCount + "," + result.executionTimeMs + "," + result.memoryUsageMB + "\n";
                    Files.write(resultsPath, resultLine.getBytes(), java.nio.file.StandardOpenOption.APPEND);
                    
                    System.gc(); // GC 유도
                    Thread.sleep(2000); // 안정화 시간
                }
                
                avgTime /= 3;
                avgMemory /= 3;
                System.out.println("  평균 실행 시간: " + String.format("%.2f", avgTime) + "ms");
                System.out.println("  평균 메모리 사용량: " + String.format("%.2f", avgMemory) + "MB");
            }
        }
        
        System.out.println("\n테스트 완료. 결과 저장 위치: " + resultsPath.toAbsolutePath());
    }

    private static TestResult runTest(boolean useMultiSheetMode, int consumerThreadCount, String filenameSuffix) throws Exception {
        long initialMemory = getUsedMemory();
        long startTime = System.currentTimeMillis();

        int actualNumSheets = useMultiSheetMode ? (int) Math.ceil((double) ROW_COUNT / SHEET_THRESHOLD) : 1;
        System.out.println("  시트 수: " + actualNumSheets + ", 소비자 스레드: " + consumerThreadCount);

        final AtomicLong totalCellsWritten = new AtomicLong(0);
        final ConcurrentLinkedQueue<CellData> dataQueue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean producerFinished = new AtomicBoolean(false);

        String filename = OUTPUT_DIR + "/excel_" + filenameSuffix + ".xlsx";
        try (FileOutputStream fos = new FileOutputStream(filename);
             Workbook workbook = new Workbook(fos, "ExcelTestCFQueue", "1.0")) {

            ConcurrentHashMap<Integer, Worksheet> worksheets = new ConcurrentHashMap<>();
            for (int i = 0; i < actualNumSheets; i++) {
                Worksheet sheet = workbook.newWorksheet("Sheet" + (i + 1));
                for (int col = 0; col < COLUMN_COUNT; col++) {
                    sheet.value(0, col, "Column " + (col + 1));
                    sheet.style(0, col).bold().fillColor("D3D3D3").set();
                    sheet.width(col, 15);
                }
                worksheets.put(i, sheet);
            }

            // 생산자와 소비자 스레드들이 공유할 ExecutorService
            // 생산자 1개 + 소비자 consumerThreadCount 개. 총 consumerThreadCount + 1 스레드가 동시에 필요할 수 있음.
            // 하지만 threadCount가 전체 시스템의 코어 수 등을 고려한 값이면, 그대로 사용해도 무방.
            // 여기서는 consumerThreadCount를 소비자 전용으로 보고, 생산자는 별도 또는 공유풀을 사용한다고 가정.
            // 간결하게 하나의 풀을 사용. 만약 consumerThreadCount가 1이면 생산자와 소비자는 순차적으로 실행될 가능성 높음.
            ExecutorService executor = Executors.newFixedThreadPool(consumerThreadCount + 1); // 생산자 스레드(1) + 소비자 스레드(consumerThreadCount)
            
            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            // 생산자 작업
            CompletableFuture<Void> producerFuture = CompletableFuture.runAsync(() -> {
                System.out.println("  생산자 스레드 " + Thread.currentThread().getName() + " 시작");
                Random random = new Random(System.currentTimeMillis());
                try {
                    for (int rGlobal = 1; rGlobal <= ROW_COUNT; rGlobal++) {
                        while (dataQueue.size() > QUEUE_THRESHOLD && !Thread.currentThread().isInterrupted()) {
                            try {
                                Thread.sleep(50); // 큐가 너무 크면 잠시 대기 (백프레셔)
                            } catch (InterruptedException e) {
                                System.err.println("생산자 스레드 인터럽트 (백프레셔 대기 중)");
                                Thread.currentThread().interrupt();
                                return; // 작업 중단
                            }
                        }
                        if (Thread.currentThread().isInterrupted()) return;

                        int sheetIdx = useMultiSheetMode ? (rGlobal - 1) / SHEET_THRESHOLD : 0;
                        int rowInSheet = (rGlobal - 1) % SHEET_THRESHOLD + 1 ;

                        for (int c = 0; c < COLUMN_COUNT; c++) {
                            int value;
                            value = (int) (random.nextDouble() * 10000);

                            dataQueue.offer(new CellData(sheetIdx, rowInSheet, c, value));
                        }

                        if (rGlobal % 200_000 == 0) {
                            System.out.println("  생산자: " + rGlobal + " 행 데이터 생성 완료. 현재 큐 크기: " + dataQueue.size());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("생산자 작업 중 예외 발생: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    producerFinished.set(true);
                    System.out.println("  생산자 스레드 " + Thread.currentThread().getName() + " 완료. 최종 큐 크기: " + dataQueue.size());
                }
            }, executor);
            allFutures.add(producerFuture);

            // 소비자 작업들
            for (int i = 0; i < consumerThreadCount; i++) {
                final int consumerId = i;
                CompletableFuture<Void> consumerFuture = CompletableFuture.runAsync(() -> {
                    System.out.println("  소비자 스레드 " + Thread.currentThread().getName() + " (ID: " + consumerId + ") 시작");
                    CellData cellData;
                    long cellsProcessedByThisConsumer = 0;
                    try {
                        while (!producerFinished.get() || !dataQueue.isEmpty()) {
                            cellData = dataQueue.poll();
                            if (cellData != null) {
                                Worksheet ws = worksheets.get(cellData.sheetIndex);
                                ws.value(cellData.rowInSheet, cellData.col, cellData.value);
                                long currentTotalCells = totalCellsWritten.incrementAndGet();
                                cellsProcessedByThisConsumer++;

                                if (currentTotalCells % 1_000_000 == 0) {
                                    System.out.println("  총 셀 쓰기 진행: " + currentTotalCells + " / " + (long)ROW_COUNT * COLUMN_COUNT +
                                                       " (현재 큐 크기: " + dataQueue.size() + ", 소비자 " + consumerId + ")");
                                }
                            } else if (producerFinished.get() && dataQueue.isEmpty()) {
                                break; // 생산자 완료 및 큐 비었으면 종료
                            } else {
                                // 큐는 비었지만 생산자가 아직 작업 중일 수 있으므로 잠시 대기 (busy-waiting 방지)
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    System.err.println("소비자 스레드 " + consumerId + " 인터럽트 (대기 중)");
                                    Thread.currentThread().interrupt();
                                    break; // 작업 중단
                                }
                            }
                            if (Thread.currentThread().isInterrupted()) break;
                        }
                    } catch (Exception e) {
                        System.err.println("소비자 스레드 " + consumerId + " 작업 중 예외 발생: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        System.out.println("  소비자 스레드 " + Thread.currentThread().getName() + " (ID: " + consumerId + ") 완료. 이 스레드가 처리한 셀: " + cellsProcessedByThisConsumer);
                    }
                }, executor);
                allFutures.add(consumerFuture);
            }

            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get(); // 모든 생산/소비자 작업 완료 대기
            
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            workbook.finish();
        }

        long endTime = System.currentTimeMillis();
        long executionTimeMs = endTime - startTime;
        long finalMemory = getUsedMemory();
        double memoryUsageMB = (finalMemory - initialMemory) / (1024.0 * 1024.0);

        System.out.println("  실행 시간: " + executionTimeMs + "ms");
        System.out.println("  메모리 사용량: " + String.format("%.2f", memoryUsageMB) + "MB");
        System.out.println("  총 " + totalCellsWritten.get() + " 셀 쓰기 완료.");

        return new TestResult(executionTimeMs, memoryUsageMB);
    }

    private static void createOutputDirectory() throws IOException {
        Path dirPath = Paths.get(OUTPUT_DIR);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            System.out.println("출력 디렉토리 생성: " + dirPath.toAbsolutePath());
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static class TestResult {
        final long executionTimeMs;
        final double memoryUsageMB;

        public TestResult(long executionTimeMs, double memoryUsageMB) {
            this.executionTimeMs = executionTimeMs;
            this.memoryUsageMB = memoryUsageMB;
        }
    }
}
