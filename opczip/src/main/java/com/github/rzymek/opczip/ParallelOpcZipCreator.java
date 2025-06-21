package com.github.rzymek.opczip;

import static org.apache.commons.compress.archivers.zip.ZipArchiveEntryRequest.createZipArchiveEntryRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

import org.apache.commons.compress.archivers.zip.*;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.compress.parallel.ScatterGatherBackingStore;
import org.apache.commons.compress.parallel.ScatterGatherBackingStoreSupplier;
import org.apache.commons.io.IOUtils;


public class ParallelOpcZipCreator {

    private final Deque<ScatterZipOutputStream> streams = new ConcurrentLinkedDeque<>();
    private final ExecutorService executorService;
    private final ScatterGatherBackingStoreSupplier backingStoreSupplier;

    private final Deque<Future<ScatterZipOutputStream>> futures = new ConcurrentLinkedDeque<>();

    private final int compressionLevel;

    private final ThreadLocal<ScatterZipOutputStream> tlScatterStreams = new ThreadLocal<ScatterZipOutputStream>() {
        @Override
        protected ScatterZipOutputStream initialValue() {
            try {
                final ScatterZipOutputStream scatterStream = createDeferred(backingStoreSupplier);
                streams.add(scatterStream);
                return scatterStream;
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    /**
     * 기본 스레드 풀(가용 프로세서 수)을 사용하여 인스턴스를 생성합니다.
     */
    public ParallelOpcZipCreator() {
        this(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
    }

    /**
     * 지정된 ExecutorService를 사용하여 인스턴스를 생성합니다.
     *
     * @param executorService 병렬 작업에 사용할 ExecutorService. 이 클래스 사용 후 종료됩니다.
     */
    public ParallelOpcZipCreator(final ExecutorService executorService) {
        this(executorService, new DefaultBackingStoreSupplier(null), Deflater.DEFAULT_COMPRESSION);
    }

    /**
     * 지정된 ExecutorService와 압축 레벨로 인스턴스를 생성합니다.
     *
     * @param executorService The executorService to use.
     * @param compressionLevel The compression level, see {@link Deflater}.
     */
    public ParallelOpcZipCreator(final ExecutorService executorService, final int compressionLevel) {
        this(executorService, new DefaultBackingStoreSupplier(null), compressionLevel);
    }


    /**
     * 모든 설정을 지정하여 인스턴스를 생성합니다.
     *
     * @param executorService      병렬 작업에 사용할 ExecutorService.
     * @param backingStoreSupplier 임시 데이터 저장을 위한 Supplier.
     * @param compressionLevel     압축 레벨 (Deflater.DEFAULT_COMPRESSION, 0-9).
     */
    public ParallelOpcZipCreator(final ExecutorService executorService, final ScatterGatherBackingStoreSupplier backingStoreSupplier, final int compressionLevel) {
        if ((compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION) && compressionLevel != Deflater.DEFAULT_COMPRESSION) {
            throw new IllegalArgumentException("Compression level is expected between -1 and 9");
        }
        this.backingStoreSupplier = backingStoreSupplier;
        this.executorService = executorService;
        this.compressionLevel = compressionLevel;
    }

    /**
     * 압축할 엔트리를 작업 큐에 추가합니다.
     *
     * @param zipArchiveEntry 추가할 ZIP 엔트리 정보.
     * @param source          엔트리의 데이터 소스.
     */
    public void addArchiveEntry(final ZipArchiveEntry zipArchiveEntry, final InputStreamSupplier source) {
        submit(createCallable(zipArchiveEntry, source));
    }

    /**
     * 압축할 엔트리를 작업 큐에 추가합니다.
     *
     * @param zipArchiveEntryRequestSupplier 추가할 ZIP 엔트리 요청을 제공하는 Supplier.
     */
    public void addArchiveEntry(final ZipArchiveEntryRequestSupplier zipArchiveEntryRequestSupplier) {
        submit(createCallable(zipArchiveEntryRequestSupplier));
    }

    private Callable<ScatterZipOutputStream> createCallable(final ZipArchiveEntry zipArchiveEntry, final InputStreamSupplier source) {
        final ZipArchiveEntryRequest zipArchiveEntryRequest = createZipArchiveEntryRequest(zipArchiveEntry, source);
        return () -> {
            final ScatterZipOutputStream scatterStream = tlScatterStreams.get();
            scatterStream.addArchiveEntry(zipArchiveEntryRequest);
            return scatterStream;
        };
    }

    private Callable<ScatterZipOutputStream> createCallable(final ZipArchiveEntryRequestSupplier zipArchiveEntryRequestSupplier) {
        return () -> {
            final ScatterZipOutputStream scatterStream = tlScatterStreams.get();
            scatterStream.addArchiveEntry(zipArchiveEntryRequestSupplier.get());
            return scatterStream;
        };
    }

    /**
     * 작업을 ExecutorService에 제출합니다.
     *
     * @param callable 실행할 작업.
     */
    public void submit(final Callable<ScatterZipOutputStream> callable) {
        futures.add(executorService.submit(callable));
    }

    /**
     * 병렬로 압축된 모든 데이터를 최종 ZipArchiveOutputStream에 씁니다.
     * 이 메소드 호출 후에는 더 이상 엔트리를 추가할 수 없습니다.
     *
     * @param targetStream 최종 ZIP 파일이 될 출력 스트림.
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void writeTo(final ZipArchiveOutputStream targetStream) throws IOException, InterruptedException, ExecutionException {
        try {
            for (final Future<ScatterZipOutputStream> future : futures) {
                future.get();
            }

            for (final Future<ScatterZipOutputStream> future : futures) {
                final ScatterZipOutputStream scatterStream = future.get();
                scatterStream.zipEntryWriter().writeNextZipEntry(targetStream);
            }

        } finally {
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
            for (final ScatterZipOutputStream scatterStream : streams) {
                IOUtils.closeQuietly(scatterStream);
            }
        }
    }

    private ScatterZipOutputStream createDeferred(final ScatterGatherBackingStoreSupplier scatterGatherBackingStoreSupplier) throws IOException {
        final ScatterGatherBackingStore bs = scatterGatherBackingStoreSupplier.get();
        final StreamCompressor sc = StreamCompressor.create(compressionLevel, bs);
        return new ScatterZipOutputStream(bs, sc);
    }
}
