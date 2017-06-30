package ru.krupt.ossr;

import lombok.extern.slf4j.Slf4j;
import ru.krupt.ossr.schema.SchemaUtils;
import ru.krupt.ossr.script.Command;
import ru.krupt.ossr.script.CommandFactory;
import ru.krupt.ossr.script.CommandSubType;
import ru.krupt.ossr.script.CommandTypes;
import ru.krupt.ossr.script.ExecutionFailedException;
import ru.krupt.ossr.script.ScriptParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@SuppressWarnings("WeakerAccess")
public class TestApplication {

    private final Path patchDir;
    private final Path bakDir;

    private final Map<String, Connection> schemas = new HashMap<>();
    private final BufferedReader input = new BufferedReader(new InputStreamReader(System.in));

    public static void main(String[] args) throws URISyntaxException, IOException {
        new TestApplication().run();
    }

    public TestApplication() throws URISyntaxException {
        patchDir = Paths.get(TestApplication.class.getResource("/testPatch").toURI());
        bakDir = Paths.get(patchDir.toString() + File.separator + "bak");
        //noinspection ResultOfMethodCallIgnored
        bakDir.toFile().mkdirs();
    }

    private void run() throws IOException {
        Files.list(patchDir)
                .filter(path -> !Files.isDirectory(path))
                /*.skip(16)
                .limit(1)*/
                .forEach(this::processFile);
        schemas.values().forEach(connection -> {
            //noinspection EmptyTryBlock
            try (Connection ignored = connection) {
            } catch (SQLException e) {
                // Ignore
            }
        });
    }

    private void processFile(Path path) throws ExecutionFailedException {
        final String fileName = path.getFileName().toString();
        final String schema = SchemaUtils.getSchemaFromFileName(fileName);
        log.info("Processing {}", fileName);
        final Connection connection = schemas.computeIfAbsent(schema, key -> {
            System.out.println("Введите пароль для схемы " + key + ":");
            try {
                final String password = input.readLine();
                final Connection newConnection = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:1567/ora12",
                        key, password);
                newConnection.setAutoCommit(false);
                return newConnection;
            } catch (IOException | SQLException e) {
                log.error("Error getting password or connecting to database", e);
            }
            throw new ExecutionFailedException("Error getting password or connecting to database");
        });
        final long start = System.currentTimeMillis();
        final Iterable<Command> commands;
        try {
            commands = ScriptParser.parseFile(new FileInputStream(path.toFile()));
        } catch (FileNotFoundException e) {
            log.error("Couldn't read file {}", path, e);
            throw new ExecutionFailedException(e.toString());
        }
        try (Statement statement = connection.createStatement()) {
            boolean needCommit = false;
            for (Command command : commands) {
                log.debug("Executing:\n{}", command);
                int rowsAffected = 0;
                if (command.getSubType() == CommandSubType.INSERT || command.getSubType() == CommandSubType.UPDATE
                        || command.getSubType() == CommandSubType.DELETE) {
                    rowsAffected = statement.executeUpdate(command.getText());
                    needCommit = true;
                } else {
                    statement.execute(command.getText());
                    final SQLWarning warnings = statement.getWarnings();
                    if (warnings != null) {
                        log.warn("{}", warnings);
                    }
                    if (command.getType() == CommandTypes.ANONYMOUS_PLSQL_BLOCK) {
                        needCommit = true;
                    }
                }
                log.info(CommandFactory.getCommandResultDescription(command, rowsAffected));
            }
            if (needCommit) {
                connection.commit();
                log.info("Commit complete");
            }
        } catch (SQLException e) {
            log.error("Query execution failed", e);
            throw new ExecutionFailedException(e.toString());
        }
        final double elapsedMillis = (System.currentTimeMillis() - start) / 1000d;
        log.info("File {} executed in {} seconds", fileName, elapsedMillis);
        try {
            Files.move(path, bakDir.resolve(fileName));
        } catch (IOException e) {
            log.error("Couldn't move file to bak directory", e);
        }
    }
}
