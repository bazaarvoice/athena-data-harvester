package com.bazaarvoice.nn.nataraja.athenadataharvester.util;

import com.bazaarvoice.nn.nataraja.athenadataharvester.exception.TaskFailureException;
import com.bazaarvoice.nn.nataraja.athenadataharvester.model.HarvestTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskFileCreator {

    private ObjectMapper _mapper;
    private HarvestTask _task;
    private FileManager _fileManager;
    //map of paths to whether any actual data has been written to that path
    private final Map<Path, Boolean> _files = Maps.newHashMap();

    public TaskFileCreator(ObjectMapper mapper, HarvestTask task, FileManager fileManager) {
        _mapper = mapper;
        _task = task;
        _fileManager = fileManager;
    }

    /**
     * returns only the paths that data has been written to
     */
    public List<Path> getFiles() {
        return _files.entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public WrappedSequenceWriter openFileForArray()
            throws IOException, TaskFailureException {
        final Path path = newTaskFile(false);
        return new WrappedSequenceWriter(
                _mapper.writer().writeValuesAsArray(Files.newOutputStream(path)),
                written -> _files.put(path, written)
        );
    }

    public void saveObject(Object object)
            throws TaskFailureException {
        final Path taskFile = newTaskFile(true);
        try (OutputStream os = Files.newOutputStream(taskFile)) {
            _mapper.writeValue(os, object);
        } catch (IOException e) {
            throw new TaskFailureException(String.format("could not save object for task %s to %s", _task.taskId().toString(), taskFile.toString()), e);
        }
    }

    private Path newTaskFile(boolean isWritten)
            throws TaskFailureException {
        final String fileName = Integer.toString(_files.size());
        try {
            final Path taskFile = _fileManager.getTaskFile(_task, fileName);
            _files.put(taskFile, isWritten);
            return taskFile;
        } catch (IOException e) {
            throw new TaskFailureException(String.format("could not get file %s for task %s", fileName, _task.taskId().toString()), e);
        }
    }

}
