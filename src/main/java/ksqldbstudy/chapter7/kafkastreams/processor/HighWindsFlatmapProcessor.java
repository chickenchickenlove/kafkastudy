package ksqldbstudy.chapter7.kafkastreams.processor;

import ksqldbstudy.chapter7.domain.Power;
import ksqldbstudy.chapter7.domain.TurbineState;
import ksqldbstudy.chapter7.domain.Type;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class HighWindsFlatmapProcessor implements Processor<String, TurbineState, String, TurbineState> {

    private ProcessorContext<String, TurbineState> context;


    @Override
    public void init(ProcessorContext<String, TurbineState> context) {
        this.context = context;
    }

    // 사실 상, flatmap 연산임. 하나의 레코드로 2개의 레코드를 생성했기 때문임.
    @Override
    public void process(Record<String, TurbineState> record) {
        TurbineState reportedValue = record.value();
        context.forward(record); // downstream

        if (reportedValue.getWindSpeedMph() > 65 && reportedValue.getPower().equals(Power.ON)) {
            TurbineState desired = reportedValue.clone();
            desired.setType(Type.DESIRED);
            desired.setPower(Power.OFF);

            Record<String, TurbineState> newRecord = new Record<>(record.key(), desired, record.timestamp());
            context.forward(newRecord); //downstream
        }
    }

    @Override
    public void close() {
    }
}
