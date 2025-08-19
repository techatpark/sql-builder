package com.techatpark;

import com.techatpark.sql.ParamMapper;
import com.techatpark.sql.RowMapper;
import com.techatpark.sql.Sql;
import com.techatpark.sql.StatementMapper;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * SQL Builder should not depend on any external libraries or
 * specific jdbc implementation.
 */
class CodeReviewTests {
    @Test
    void architecture_rules() {
        JavaClasses importedClasses = new ClassFileImporter()
                .withImportOption(
                        new com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests())
                .importPackages("com.techatpark");

        ArchRule rule = classes().that()
                .resideInAnyPackage("com.techatpark")
                .should()
                    .onlyAccessClassesThat()
                        .belongToAnyOf(SQLFeatureNotSupportedException.class,
                                ResultSet.class,
                                DataSource.class,
                                Connection.class,
                                Statement.class,
                                CallableStatement.class,
                                PreparedStatement.class,
                                SQLException.class,
                                Function.class,
                                Throwable.class,
                                BigDecimal.class,
                                Sql.class,
                                RowMapper.class,
                                StatementMapper.class,
                                ParamMapper.class,
                                SqlBuilder.class,
                                Boolean.class,
                                Integer.class,
                                Double.class,
                                Float.class,
                                Boolean.class,
                                Long.class,
                                Short.class,
                                Byte.class,
                                Object.class,
                                ArrayList.class,
                                List.class,
                                Iterator.class,
                                UnsupportedOperationException.class) ;// see next section

        rule.check(importedClasses);
    }
}
