package com.techatpark;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
                        .belongToAnyOf(ResultSet.class,
                                DataSource.class,
                                Connection.class,
                                PreparedStatement.class,
                                SQLException.class,
                                Function.class,
                                Throwable.class,
                                BigDecimal.class,
                                Sql.class,
                                SqlBuilder.class,
                                Transaction.class,
                                Boolean.class,
                                Integer.class,
                                Double.class,
                                Float.class,
                                Boolean.class,
                                Long.class,
                                Short.class,
                                Object.class,
                                ArrayList.class,
                                List.class,
                                Iterator.class) ;// see next section

        rule.check(importedClasses);
    }
}
