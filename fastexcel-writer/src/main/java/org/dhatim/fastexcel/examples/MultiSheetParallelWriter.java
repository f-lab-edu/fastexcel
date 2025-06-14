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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 다중 시트 병렬 엑셀 쓰기 성능 비교 테스트 (생산자-소비자 패턴)
 * 
 * 이 테스트는 데이터를 큐에 저장하고, 여러 스레드가 큐에서 데이터를 가져와
 * 행 번호를 100만으로 나눈 나머지에 따라 다른 시트에 동시다발적으로 데이터를 쓰는 방식과
 * 단일 시트에 데이터를 쓰는 방식의 성능을 비교합니다.
 */
public class MultiSheetParallelWriter {

    // 테스트 설정
    private static final int COLUMN_COUNT = 30;
    private static final int ROW_COUNT = 2_000_000; // 총 행의 수
    private static final int SHEET_THRESHOLD = 1_000_000; // 시트 당 행 개수 제한
    private static final int[] THREAD_COUNTS = {1, 2}; // 테스트할 스레드 수
    private static final boolean[] USE_MULTI_SHEET = {false, true}; // 단일 시트 vs 다중 시트
    private static final String OUTPUT_DIR = "excel_test_results";
    private static final int QUEUE_THRESHOLD = 500_000; // 큐 크기 임계값 (셀 개수 기준)
    
    // 데이터 생성을 위한 변수
    private static final String[] STRING_VALUES = {
            "텍스트 데이터", "샘플 값", "테스트 문자열", "엑셀 테스트", "다중 스레드", 
            "성능 테스트", "대용량 데이터", "FastExcel", "Java 테스트", "병렬 처리"
    };
    
    public static void main(String[] args) throws Exception {
        System.out.println("다중 시트 병렬 엑셀 쓰기 성능 비교 테스트 (생산자-소비자 패턴) 시작");
        createOutputDirectory();
        
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path resultsPath = Paths.get(OUTPUT_DIR, "multi_sheet_queue_results_" + timestamp + ".txt");
        
        Files.write(resultsPath, "시트 방식,스레드 수,실행 시간(ms),메모리 사용량(MB)\n".getBytes());
        
        for (boolean multiSheet : USE_MULTI_SHEET) {
            String sheetMode = multiSheet ? "다중 시트" : "단일 시트";
            
            for (int threadCount : THREAD_COUNTS) {
                System.out.println("\n" + sheetMode + ", 스레드 수 " + threadCount + "로 테스트 시작");
                
                double avgTime = 0;
                double avgMemory = 0;
                
                for (int i = 0; i < 3; i++) {
                    System.out.println("  반복 #" + (i + 1));
                    TestResult result = runTest(multiSheet, threadCount, 
                            timestamp + "_" + (multiSheet ? "multi" : "single") + "_thread" + threadCount + "_run" + (i + 1));
                    avgTime += result.executionTimeMs;
                    avgMemory += result.memoryUsageMB;
                    
                    String resultLine = sheetMode + "," + threadCount + "," + result.executionTimeMs + "," + result.memoryUsageMB + "\n";
                    Files.write(resultsPath, resultLine.getBytes(), java.nio.file.StandardOpenOption.APPEND);
                    
                    System.gc();
                    Thread.sleep(2000);
                }
                
                avgTime /= 3;
                avgMemory /= 3;
                System.out.println("  평균 실행 시간: " + avgTime + "ms");
                System.out.println("  평균 메모리 사용량: " + avgMemory + "MB");
            }
        }
        
        System.out.println("\n테스트 완료. 결과 저장 위치: " + resultsPath.toAbsolutePath());
    }

    /**
     * 셀 데이터 저장을 위한 내부 클래스
     */
    private static class CellData {
        final int sheetIndex; // 대상 시트 인덱스
        final int sheetRow;   // 시트 내 행 번호 (1부터 시작)
        final int col;        // 열 번호 (0부터 시작)
        final boolean isNumeric;
        final double numericValue;
        final String stringValue;

        public CellData(int sheetIndex, int sheetRow, int col, double value) {
            this.sheetIndex = sheetIndex;
            this.sheetRow = sheetRow;
            this.col = col;
            this.isNumeric = true;
            this.numericValue = value;
            this.stringValue = null;
        }

        public CellData(int sheetIndex, int sheetRow, int col, String value) {
            this.sheetIndex = sheetIndex;
            this.sheetRow = sheetRow;
            this.col = col;
            this.isNumeric = false;
            this.numericValue = 0;
            this.stringValue = value;
        }
    }
    
    private static TestResult runTest(boolean useMultiSheet, int threadCount, String filenameSuffix) throws Exception {
        long initialMemory = getUsedMemory();
        long startTime = System.currentTimeMillis();

        int numSheets = (int) Math.ceil((double) ROW_COUNT / SHEET_THRESHOLD);
        System.out.println("  시트 수: " + numSheets);

        ConcurrentLinkedQueue<CellData> dataQueue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean producerFinished = new AtomicBoolean(false);
        final AtomicInteger queueSize = new AtomicInteger(0); // 큐에 있는 셀 데이터 개수
        final AtomicLong totalCellsWritten = new AtomicLong(0);
        final CountDownLatch writerLatch = new CountDownLatch(threadCount);

        String filename = OUTPUT_DIR + "/excel_" + filenameSuffix + ".xlsx";
        Workbook workbook = new Workbook(new FileOutputStream(filename), "ExcelTest", "1.0");

        ConcurrentHashMap<Integer, Worksheet> worksheets = new ConcurrentHashMap<>();
        for (int i = 0; i < numSheets; i++) {
            Worksheet sheet = workbook.newWorksheet("Sheet" + (i + 1));
            for (int col = 0; col < COLUMN_COUNT; col++) {
                sheet.value(0, col, "Column " + (col + 1));
                sheet.style(0, col).bold().fillColor("D3D3D3").set();
                sheet.width(col, 15);
            }
            worksheets.put(i, sheet);
        }
        
        // 데이터 생성 스레드 (Producer)
        Thread producerThread = new Thread(() -> {
            try {
                Random random = new Random();
                for (int r = 1; r <= ROW_COUNT; r++) { // r은 1부터 시작하는 실제 행 번호
                    for (int c = 0; c < COLUMN_COUNT; c++) {
                        int currentSheetIndex;
                        int currentRowInSheet;

                        currentSheetIndex = (r - 1) / SHEET_THRESHOLD; // 0부터 시작하는 시트 인덱스
                        currentRowInSheet = (r - 1) % SHEET_THRESHOLD + 1; // 시트 내 1부터 시작하는 행 번호

                        CellData cellData;
                        if (random.nextBoolean()) {
                            double value = random.nextDouble() * 10000;
                            cellData = new CellData(currentSheetIndex, currentRowInSheet, c, value);
                        } else {
                            String value = STRING_VALUES[random.nextInt(STRING_VALUES.length)];
                            cellData = new CellData(currentSheetIndex, currentRowInSheet, c, value);
                        }
                        dataQueue.add(cellData);
                        
                        int currentQueueSize = queueSize.incrementAndGet();
                        if (currentQueueSize > QUEUE_THRESHOLD) {
                            while (queueSize.get() > QUEUE_THRESHOLD / 2) {
                                Thread.sleep(10); // 백프레셔: 큐가 너무 크면 생산자 대기
                            }
                        }
                    }
                    if (r % 100_000 == 0) {
                        System.out.println("  데이터 생성 진행: " + r + "/" + ROW_COUNT + " 행, 현재 큐 크기: " + queueSize.get());
                    }
                }
                producerFinished.set(true);
                System.out.println("  데이터 생성 완료. 총 " + (ROW_COUNT * COLUMN_COUNT) + " 셀 생성됨. 최종 큐 크기: " + queueSize.get());
            } catch (Exception e) {
                e.printStackTrace();
            } 
        });
        producerThread.start();
        
        // 라이터 스레드 (Consumer)
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                System.out.println("  라이터 스레드 " + threadId + " 시작");
                try {
                    while (!producerFinished.get() || !dataQueue.isEmpty()) {
                        CellData data = dataQueue.poll();
                        if (data != null) {
                            Worksheet sheet = worksheets.get(data.sheetIndex);
                            if (data.isNumeric) {
                                sheet.value(data.sheetRow, data.col, data.numericValue);
                            } else {
                                sheet.value(data.sheetRow, data.col, data.stringValue);
                            }
                            queueSize.decrementAndGet();
                            long written = totalCellsWritten.incrementAndGet();
                            if (written % 1_000_000 == 0) {
                                System.out.println("  쓰기 진행: " + 
                                    String.format("%.2f", (double)written * 100 / (ROW_COUNT * COLUMN_COUNT)) + 
                                    "% 완료 (" + written + " 셀)");
                            }
                        } else if (producerFinished.get() && dataQueue.isEmpty()) {
                            break; // 생산자 완료되고 큐가 비면 종료
                        } else {
                            Thread.sleep(1); // 큐가 비었지만 생산자가 아직 실행 중이면 잠시 대기
                        }
                    }
                    System.out.println("  라이터 스레드 " + threadId + " 완료. 남은 큐 크기: " + dataQueue.size());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    writerLatch.countDown();
                }
            });
        }
        
        // 모니터링 스레드
        Thread monitorThread = new Thread(() -> {
            try {
                while (writerLatch.getCount() > 0) {
                    System.out.println("    작업 진행 중... 큐 크기: " + queueSize.get() + 
                            ", 작성된 셀: " + totalCellsWritten.get() + 
                            " / " + (ROW_COUNT * COLUMN_COUNT) + 
                            " (" + String.format("%.2f", (double)totalCellsWritten.get() * 100 / (ROW_COUNT * COLUMN_COUNT)) + "%)");
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                // 모니터링 중단됨
            }
        });
        monitorThread.start();
        
        writerLatch.await(); // 모든 라이터 스레드 완료 대기
        monitorThread.interrupt(); // 모니터링 스레드 종료
        producerThread.join(); // 생산자 스레드 완료 대기 (선택 사항, 이미 producerFinished로 제어)
        
        workbook.close();
        executor.shutdown();
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        long memoryUsage = (getUsedMemory() - initialMemory) / (1024 * 1024);
        
        System.out.println("\n  테스트 완료:");
        System.out.println("    총 행 수: " + ROW_COUNT);
        System.out.println("    시트 수: " + numSheets);
        System.out.println("    스레드 수: " + threadCount);
        System.out.println("    실행 시간: " + executionTime + "ms");
        System.out.println("    메모리 사용량: " + memoryUsage + "MB");
        
        return new TestResult(executionTime, memoryUsage);
    }
    
    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
    
    private static void createOutputDirectory() throws IOException {
        Path path = Paths.get(OUTPUT_DIR);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
    
    private static class TestResult {
        final long executionTimeMs;
        final long memoryUsageMB;
        
        public TestResult(long executionTimeMs, long memoryUsageMB) {
            this.executionTimeMs = executionTimeMs;
            this.memoryUsageMB = memoryUsageMB;
        }
    }
}
