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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 다중 스레드 엑셀 쓰기 최적화 테스트
 * 
 * 이 테스트는 FastExcel 라이브러리를 사용해 대용량 엑셀 파일 쓰기 성능을 개선하기 위해
 * 큐와 다중 스레드 라이터를 활용한 접근법을 테스트합니다.
 * 실험 계획서에 따라 구현되었습니다.
 */
public class FirstMultithreadedExcelTest {
    // 테스트 설정
    private static final int COLUMN_COUNT = 70;
    private static final int ROW_COUNT = 1_000_000;
    private static final int QUEUE_THRESHOLD = 100_000;
    private static final int[] THREAD_COUNTS = {1, 2, 3, 5}; // 테스트할 스레드 수
    private static final String OUTPUT_DIR = "excel_test_results";
    
    // 데이터 생성을 위한 변수
    private static final String[] STRING_VALUES = {
            "텍스트 데이터", "샘플 값", "테스트 문자열", "엑셀 테스트", "다중 스레드", 
            "성능 테스트", "대용량 데이터", "FastExcel", "Java 테스트", "병렬 처리"
    };
    
    public static void main(String[] args) throws Exception {
        System.out.println("다중 스레드 엑셀 쓰기 최적화 테스트 시작");
        createOutputDirectory();
        
        // 실험 결과를 저장할 경로 생성
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path resultsPath = Paths.get(OUTPUT_DIR, "results_" + timestamp + ".txt");
        
        // 결과 파일 헤더 작성
        Files.write(resultsPath, "스레드 수,실행 시간(ms),메모리 사용량(MB)\n".getBytes());
        
        // 다양한 스레드 수로 테스트 실행
        for (int threadCount : THREAD_COUNTS) {
            System.out.println("\n스레드 수 " + threadCount + "로 테스트 시작");
            
            // 테스트 3회 반복
            double avgTime = 0;
            double avgMemory = 0;
            
            for (int i = 0; i < 3; i++) {
                System.out.println("  반복 #" + (i + 1));
                TestResult result = runTest(threadCount, timestamp + "_thread" + threadCount + "_run" + (i + 1));
                avgTime += result.executionTimeMs;
                avgMemory += result.memoryUsageMB;
                
                // 결과 기록
                String resultLine = threadCount + "," + result.executionTimeMs + "," + result.memoryUsageMB + "\n";
                Files.write(resultsPath, resultLine.getBytes(), java.nio.file.StandardOpenOption.APPEND);
                
                // 메모리 정리를 위한 가비지 컬렉션 실행
                System.gc();
//                Thread.sleep(2000);
            }
            
            // 평균 계산 및 출력
            avgTime /= 3;
            avgMemory /= 3;
            System.out.println("  평균 실행 시간: " + avgTime + "ms");
            System.out.println("  평균 메모리 사용량: " + avgMemory + "MB");
        }
        
        System.out.println("\n테스트 완료. 결과 저장 위치: " + resultsPath.toAbsolutePath());
    }
    
    /**
     * 지정된 스레드 수로 엑셀 생성 테스트 실행
     * @param threadCount 사용할 스레드 수
     * @param filenameSuffix 생성될 엑셀 파일 접미사
     * @return 테스트 실행 결과
     */
    private static TestResult runTest(int threadCount, String filenameSuffix) throws Exception {
        // 메모리 사용량 측정을 위한 초기값
        long initialMemory = getUsedMemory();
        long startTime = System.currentTimeMillis();
        
        // 데이터 저장을 위한 큐 생성
        ConcurrentLinkedQueue<CellData> dataQueue = new ConcurrentLinkedQueue<>();
        
        // 스레드 제어용 변수들
        final AtomicBoolean producerFinished = new AtomicBoolean(false);
        final AtomicInteger queueSize = new AtomicInteger(0);
        final AtomicLong totalCellsWritten = new AtomicLong(0);
        final CountDownLatch writerLatch = new CountDownLatch(threadCount);
        
        // 엑셀 파일 생성
        String filename = OUTPUT_DIR + "/excel_" + filenameSuffix + ".xlsx";
        Workbook workbook = new Workbook(new FileOutputStream(filename), "FastExcelTest", "1.0");
        
        // 워크시트 생성
        Worksheet worksheet = workbook.newWorksheet("Data");
        
        // 열 너비 설정
        for (int i = 0; i < COLUMN_COUNT; i++) {
            worksheet.width(i, 15);
        }
        
        // 헤더 행 추가
        for (int i = 0; i < COLUMN_COUNT; i++) {
            worksheet.value(0, i, "Column " + (i + 1));
            worksheet.style(0, i).bold().fillColor("D3D3D3").set();
        }
        
        // 데이터 생성 스레드
        Thread producerThread = new Thread(() -> {
            try {
                Random random = new Random();
                
                for (int row = 1; row <= ROW_COUNT; row++) {
                    for (int col = 0; col < COLUMN_COUNT; col++) {
                        // 50%는 숫자 데이터, 50%는 문자열 데이터
                        if (random.nextBoolean()) {
                            double value = random.nextDouble() * 10000;
                            dataQueue.add(new CellData(row, col, value));
                        } else {
                            String value = STRING_VALUES[random.nextInt(STRING_VALUES.length)];
                            dataQueue.add(new CellData(row, col, value));
                        }
                        
                        // 큐 크기 증가 및 모니터링
                        int currentSize = queueSize.incrementAndGet();
                        if (currentSize > QUEUE_THRESHOLD) {
                            // 큐 크기가 임계값을 초과하면 대기
                            while (queueSize.get() > QUEUE_THRESHOLD / 2) {
                                Thread.sleep(10);
                            }
                        }
                    }
                    
                    if (row % 100000 == 0) {
                        System.out.println("  데이터 생성 진행: " + row + "/" + ROW_COUNT + " 행");
                    }
                }
                
                // 데이터 생성 완료
                producerFinished.set(true);
                System.out.println("  데이터 생성 완료. 총 " + (ROW_COUNT * COLUMN_COUNT) + " 셀 생성됨");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 라이터 스레드 생성 (스레드 수에 따라 다름)
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    while (!producerFinished.get() || !dataQueue.isEmpty()) {
                        CellData data = dataQueue.poll();
                        if (data != null) {
                            // 셀에 데이터 쓰기
                            if (data.isNumeric) {
                                worksheet.value(data.row, data.col, data.numericValue);
                            } else {
                                worksheet.value(data.row, data.col, data.stringValue);
                            }
                            
                            // 큐 크기 감소 및 쓰기 카운트 증가
                            queueSize.decrementAndGet();
                            totalCellsWritten.incrementAndGet();
                            
                            if (totalCellsWritten.get() % 1000000 == 0) {
                                System.out.println("  쓰기 진행: " + 
                                        String.format("%.2f", (double)totalCellsWritten.get() * 100 / (ROW_COUNT * COLUMN_COUNT)) + 
                                        "% 완료 (" + totalCellsWritten.get() + " 셀)");
                            }
                        } else {
                            // 큐가 빈 경우 짧게 대기
                            Thread.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    writerLatch.countDown();
                }
            });
        }
        
        // 데이터 생성 스레드 시작
        producerThread.start();
        
        // 모니터링 스레드
        Thread monitorThread = new Thread(() -> {
            try {
                while (!producerFinished.get() || !dataQueue.isEmpty()) {
                    System.out.println("  현재 큐 크기: " + queueSize.get() + 
                            ", 메모리 사용량: " + (getUsedMemory() - initialMemory) / (1024 * 1024) + "MB");
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        monitorThread.start();
        
        // 데이터 생성 스레드 완료 대기
        producerThread.join();
        
        // 모든 라이터 스레드 완료 대기
        writerLatch.await();
        
        // 모니터링 스레드 중단
        monitorThread.interrupt();
        
        // 엑셀 파일 저장 및 종료
        workbook.close();
        executor.shutdown();
        
        // 결과 계산
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        long memoryUsage = (getUsedMemory() - initialMemory) / (1024 * 1024);
        
        System.out.println("  테스트 완료: " + threadCount + " 스레드");
        System.out.println("  실행 시간: " + executionTime + "ms");
        System.out.println("  메모리 사용량: " + memoryUsage + "MB");
        
        return new TestResult(executionTime, memoryUsage);
    }
    
    /**
     * 현재 사용 중인 메모리 측정
     */
    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
    
    /**
     * 결과 디렉토리 생성
     */
    private static void createOutputDirectory() throws IOException {
        Path path = Paths.get(OUTPUT_DIR);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
    
    /**
     * 셀 데이터 저장 클래스
     */
    private static class CellData {
        final int row;
        final int col;
        final String stringValue;
        final double numericValue;
        final boolean isNumeric;
        
        // 문자열 생성자
        public CellData(int row, int col, String value) {
            this.row = row;
            this.col = col;
            this.stringValue = value;
            this.numericValue = 0;
            this.isNumeric = false;
        }
        
        // 숫자 생성자
        public CellData(int row, int col, double value) {
            this.row = row;
            this.col = col;
            this.stringValue = null;
            this.numericValue = value;
            this.isNumeric = true;
        }
    }
    
    /**
     * 테스트 결과 저장 클래스
     */
    private static class TestResult {
        final long executionTimeMs;
        final long memoryUsageMB;
        
        public TestResult(long executionTimeMs, long memoryUsageMB) {
            this.executionTimeMs = executionTimeMs;
            this.memoryUsageMB = memoryUsageMB;
        }
    }
}
