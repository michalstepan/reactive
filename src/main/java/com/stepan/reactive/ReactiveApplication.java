package com.stepan.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

@SpringBootApplication
public class ReactiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveApplication.class, args);
    }

    @RestController
    public class ReactiveController {

        @GetMapping(value = "/", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
        public Flux<String> emitStrings() {
            return Flux.fromIterable(List.of("a", "b", "c", "d", "e", "f", "g", "h"))
                    .zipWith(Flux.interval(Duration.ofSeconds(1)), (s, aLong) -> s);
        }

        @GetMapping(value = "/dir/consume", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
        public Flux<String> watchDirConsumer() {
            WebClient client = WebClient.create("http://localhost:8080");

            Flux<String> emitter = client.get()
                    .uri("/dir").accept(MediaType.TEXT_EVENT_STREAM)
                    .retrieve()
                    .bodyToFlux(String.class)
                    .log();

            return emitter
                    .map(path -> {
                        System.out.println("re-emited: " + path);
                        return path;
                    });
        }

        @GetMapping(value = "/dir", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
        public Flux<String> watchDir() {

            return Flux.create((FluxSink<String> emitter) -> {

                WatchService watcher = null;
                WatchKey key = null;
                try {
                    watcher = FileSystems.getDefault().newWatchService();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Path dir = Paths.get("");
                try {
                    key = dir.register(watcher,
                            ENTRY_CREATE);

                    System.out.println("Watching over directory " + dir.toString());
                } catch (IOException x) {
                    System.err.println(x);
                }
                for (; ; ) {

                    // wait for key to be signaled
                    try {
                        key = watcher.take();
                    } catch (InterruptedException x) {
                        emitter.error(x);
                    }

                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        WatchEvent<Path> ev = (WatchEvent<Path>) event;
                        Path filename = ev.context();

                        System.out.format("Emitting file %s%n", filename);
                        emitter.next(filename.toString());

                    }

                    // Reset the key -- this step is critical if you want to
                    // receive further watch events.  If the key is no longer valid,
                    // the directory is inaccessible so exit the loop.
                    boolean valid = key.reset();
                    if (!valid) {
                        emitter.complete();
                        break;
                    }
                }
            });
        }
    }
}