package cloud.erda.analyzer.runtime.models;

import cloud.erda.analyzer.runtime.sources.DataRowReader;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;

@Slf4j
public class DiceOrgReader implements DataRowReader<DiceOrg> {
    @Override
    public DiceOrg read(ResultSet resultSet) throws Exception {
        DiceOrg diceOrg = new DiceOrg();
        Long id = resultSet.getLong("id");
        String name = resultSet.getString("name");
        diceOrg.setId(id);
        diceOrg.setName(name);
        return diceOrg;
    }
}
