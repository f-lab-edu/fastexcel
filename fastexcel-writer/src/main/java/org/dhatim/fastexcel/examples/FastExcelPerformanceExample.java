package org.dhatim.fastexcel.examples;

import org.dhatim.fastexcel.Workbook;
import org.dhatim.fastexcel.Worksheet;
import org.dhatim.fastexcel.Color;
import org.dhatim.fastexcel.Range;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * FastExcel 성능 예제
 * 
 * 이 예제는 FastExcel의 주요 성능 최적화 기능을 보여줍니다:
 * 1. 대량 데이터 처리
 * 2. 메모리 효율성 (StringCache, StyleCache)
 * 3. 멀티스레딩 지원
 * 4. 스타일링 및 형식 지정
 */
public class FastExcelPerformanceExample {

    private static final int NUM_ROWS = 100_000;
    private static final int NUM_COLS = 10;
    private static final String[] REPEATED_STRINGS = {
            "자주 반복되는 문자열입니다",
            "이 문자열은 여러 번 사용됩니다",
            "StringCache가 이 문자열을 최적화합니다",
            "FastExcel은 메모리 효율적입니다",
            "대용량 Excel 파일 생성에 최적화되었습니다"
    };
    
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("FastExcel 성능 예제 시작...");
        long startTime = System.currentTimeMillis();
        
        // 1. 단일 워크시트 대용량 데이터 예제
        generateLargeWorksheet();
        
        // 2. 멀티스레딩 예제
        generateMultiThreadedWorkbook();
        
        long endTime = System.currentTimeMillis();
        System.out.println("총 실행 시간: " + (endTime - startTime) + "ms");
    }
    
    /**
     * 단일 워크시트에 대량의 데이터를 생성하는 예제
     */
    private static void generateLargeWorksheet() throws IOException {
        System.out.println("대용량 워크시트 생성 중...");
        long startTime = System.currentTimeMillis();
        
        try (OutputStream os = new FileOutputStream("large_worksheet_example.xlsx");
             Workbook wb = new Workbook(os, "FastExcelDemo", "1.0")) {
            
            Worksheet ws = wb.newWorksheet("대용량 데이터");
            
            // 헤더 행 스타일링
            ws.value(0, 0, "ID");
            ws.value(0, 1, "이름");
            ws.value(0, 2, "카테고리");
            ws.value(0, 3, "가격");
            ws.value(0, 4, "수량");
            ws.value(0, 5, "총액");
            ws.value(0, 6, "날짜");
            ws.value(0, 7, "반복 문자열");
            ws.value(0, 8, "랜덤 값");
            ws.value(0, 9, "설명");
            
            Range headerRange = ws.range(0, 0, 0, NUM_COLS - 1);
            headerRange.style()
                    .bold()
                    .fillColor(Color.GRAY3)
                    .horizontalAlignment("center")
                    .set();
            
            // 열 너비 설정
            for (int i = 0; i < NUM_COLS; i++) {
                ws.width(i, 15);
            }
            ws.width(9, 30); // 설명 열은 더 넓게
            
            Random random = new Random();
            
            // 대량의 데이터 추가
            for (int row = 1; row <= NUM_ROWS; row++) {
                // 고유한 데이터
                ws.value(row, 0, row); // ID
                ws.value(row, 1, "상품" + row); // 이름
                
                // 제한된 카테고리 (StyleCache 효과 보여주기)
                String category = "카테고리" + (row % 5 + 1);
                ws.value(row, 2, category);
                
                // 숫자 데이터
                double price = 1000 + (random.nextDouble() * 9000);
                int quantity = 1 + random.nextInt(10);
                ws.value(row, 3, price);
                ws.value(row, 4, quantity);
                
                // 수식
                ws.formula(row, 5, "D" + (row + 1) + "*E" + (row + 1));
                
                // 날짜
                ws.value(row, 6, LocalDateTime.now().minusDays(random.nextInt(365)));
                
                // 반복되는 문자열 (StringCache 효과 보여주기)
                ws.value(row, 7, REPEATED_STRINGS[row % REPEATED_STRINGS.length]);
                
                // 랜덤 값
                ws.value(row, 8, random.nextDouble() * 100);
                
                // 긴 설명
                ws.value(row, 9, "이 행은 ID가 " + row + "인 상품에 대한 상세 설명입니다. " +
                        "이 상품은 " + category + "에 속합니다.");
                
                // 특정 조건에 따른 스타일링 (10,000행마다 배경색 변경)
                if (row % 10000 == 0) {
                    ws.range(row, 0, row, NUM_COLS - 1).style()
                            .fillColor(Color.LIGHT_GREEN)
                            .set();
                }
                
                // 홀수/짝수 행 스타일 적용 (alternating rows)
                if (row % 2 == 0) {
                    ws.style(row, 2).fillColor(Color.GRAY2).set();
                }
                
                // 숫자 형식 지정
                ws.style(row, 3).format("#,##0.00").set();
                ws.style(row, 5).format("#,##0.00").set();
                ws.style(row, 8).format("0.00%").set();
                
                // 날짜 형식 지정
                ws.style(row, 6).format("yyyy-MM-dd").set();
            }
            
            // 첫 번째 열 고정
            ws.freezePane(1, 1);
            
            // 상단 10개 행에 대해 자동 필터 추가

        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("대용량 워크시트 생성 완료. 소요 시간: " + (endTime - startTime) + "ms");
    }
    
    /**
     * 여러 워크시트를 멀티스레드로 생성하는 예제
     */
    private static void generateMultiThreadedWorkbook() throws IOException, InterruptedException {
        System.out.println("멀티스레드 워크북 생성 중...");
        long startTime = System.currentTimeMillis();
        
        try (OutputStream os = new FileOutputStream("multithreaded_example.xlsx");
             Workbook wb = new Workbook(os, "FastExcelDemo", "1.0")) {
            
            // 워크시트 생성 (각각 다른 스레드에서 작업)
            int numThreads = 3;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            
            for (int i = 0; i < numThreads; i++) {
                final int sheetIndex = i;
                final Worksheet ws = wb.newWorksheet("시트" + (sheetIndex + 1));
                
                executor.submit(() -> {
                    try {
                        fillWorksheetWithData(ws, sheetIndex);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            
            // 모든 스레드가 완료될 때까지 대기
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("멀티스레드 워크북 생성 완료. 소요 시간: " + (endTime - startTime) + "ms");
    }
    
    /**
     * 각 워크시트를 데이터로 채우는 메서드 (각 스레드에서 호출됨)
     */
    private static void fillWorksheetWithData(Worksheet ws, int sheetIndex) {
        try {
            // 헤더 행
            ws.value(0, 0, "스레드");
            ws.value(0, 1, "행 번호");
            ws.value(0, 2, "값");
            ws.value(0, 3, "반복 문자열");
            
            ws.range(0, 0, 0, 3).style()
                    .bold()
                    .fillColor(Color.AQUA)
                    .horizontalAlignment("center")
                    .set();
            
            Random random = new Random();
            int rowsPerSheet = NUM_ROWS / 3; // 각 시트에 처리할 행 수
            
            for (int row = 1; row <= rowsPerSheet; row++) {
                ws.value(row, 0, "스레드-" + (sheetIndex + 1));
                ws.value(row, 1, row);
                ws.value(row, 2, random.nextDouble() * 1000);
                ws.value(row, 3, REPEATED_STRINGS[row % REPEATED_STRINGS.length]);
                
                // 멀티스레드 효과를 시각화하기 위한 시트별 스타일링
                if (sheetIndex == 0) {
                    ws.style(row, 0).fillColor(Color.LIGHT_GREEN).set();
                } else if (sheetIndex == 1) {
                    ws.style(row, 0).fillColor(Color.AQUA).set();
                } else {
                    ws.style(row, 0).fillColor(Color.AIR_FORCE_BLUE).set();
                }
                
                // 수치 형식 지정
                ws.style(row, 2).format("#,##0.00").set();
            }
            
            // 시트 요약 정보 추가
            int summaryRow = rowsPerSheet + 2;
            ws.value(summaryRow, 0, "시트 " + (sheetIndex + 1) + " 요약");
            ws.range(summaryRow, 0, summaryRow, 1).merge();
            ws.style(summaryRow, 0).bold().set();
            
            ws.value(summaryRow + 1, 0, "행 수:");
            ws.value(summaryRow + 1, 1, rowsPerSheet);
            
            ws.value(summaryRow + 2, 0, "평균:");
            ws.formula(summaryRow + 2, 1, "AVERAGE(C2:C" + (rowsPerSheet + 1) + ")");
            ws.style(summaryRow + 2, 1).format("#,##0.00").set();
            
            ws.value(summaryRow + 3, 0, "최대값:");
            ws.formula(summaryRow + 3, 1, "MAX(C2:C" + (rowsPerSheet + 1) + ")");
            ws.style(summaryRow + 3, 1).format("#,##0.00").set();
            
            ws.value(summaryRow + 4, 0, "최소값:");
            ws.formula(summaryRow + 4, 1, "MIN(C2:C" + (rowsPerSheet + 1) + ")");
            ws.style(summaryRow + 4, 1).format("#,##0.00").set();
            
            // 시트별 특성화
            if (sheetIndex == 0) {
                // 첫 번째 시트에는 차트용 데이터 추가
                ws.value(summaryRow + 6, 0, "차트 데이터");
                ws.style(summaryRow + 6, 0).bold().set();
                
                ws.value(summaryRow + 7, 0, "구간");
                ws.value(summaryRow + 7, 1, "빈도");
                
                for (int i = 0; i < 10; i++) {
                    ws.value(summaryRow + 8 + i, 0, i * 100 + "-" + ((i + 1) * 100));
                    ws.formula(summaryRow + 8 + i, 1, 
                            "COUNTIFS(C2:C" + (rowsPerSheet + 1) + 
                            ",\">=" + (i * 100) + "\",C2:C" + (rowsPerSheet + 1) + 
                            ",\"<" + ((i + 1) * 100) + "\")");
                }
            } else if (sheetIndex == 1) {
                // 두 번째 시트에는 테이블 추가
                Range tableRange = ws.range(summaryRow + 6, 0, summaryRow + 16, 1);
                
                ws.value(summaryRow + 6, 0, "구간");
                ws.value(summaryRow + 6, 1, "데이터");
                
                for (int i = 0; i < 10; i++) {
                    ws.value(summaryRow + 7 + i, 0, "항목-" + (i + 1));
                    ws.value(summaryRow + 7 + i, 1, random.nextDouble() * 100);
                }
                
                tableRange.createTable()
                        .setDisplayName("DataTable")
                        .setName("DataTable")
                        .styleInfo()
                        .setStyleName("TableStyleMedium2")
                        .setShowRowStripes(true);
            } else {
                // 세 번째 시트에는 조건부 서식 추가
                Range conditionalRange = ws.range(1, 2, rowsPerSheet, 2);
                

            }
            
            // 열 너비 조정
            for (int i = 0; i < 4; i++) {
                ws.width(i, 15);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
