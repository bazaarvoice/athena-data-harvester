package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import com.bazaarvoice.nn.nataraja.athenadataharvester.model.HarvestTask;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;


public class FileManager {

    private final Path _taskBaseDir;

    public FileManager(String baseDirectory) {
        final FileSystem defaultFS = FileSystems.getDefault();
        _taskBaseDir = defaultFS.getPath(baseDirectory).toAbsolutePath();
        setupBaseDirectory();
    }

    private void setupBaseDirectory() {
        if (Files.notExists(_taskBaseDir)) {
            try {
                Files.createDirectories(_taskBaseDir);
            } catch (IOException e) {
                throw new IllegalStateException("could not create combine tasks storage directory " + _taskBaseDir, e);
            }
        } else if (!Files.isDirectory(_taskBaseDir)) {
            throw new IllegalStateException("file exists at " + _taskBaseDir + " but is not a directory!");
        }
    }

    public Path getTaskFile(HarvestTask task, String fileName)
            throws IOException {
        return getTaskDirectory(task).resolve(fileName);
    }

    private Path getTaskDirectory(HarvestTask task)
            throws IOException {
        return ensureDirectory(_taskBaseDir, task.taskId().toString());
    }

    private Path ensureDirectory(Path parentDirectory, String subDirectory)
            throws IOException {
        final Path childDirectory = parentDirectory.resolve(subDirectory);
        if (Files.notExists(childDirectory)) {
            Files.createDirectory(childDirectory);
        }
        return childDirectory;
    }
}
