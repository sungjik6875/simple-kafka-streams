package com.example;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

// 스트림 프로세서 클래스를 정의하기 위해 카프카 스트림즈 라이브러리에서 제공하는
// Processor 또는 Transformer를 구현해야 한다.
public class FilterProcessor implements Processor<String, String> {
    // 프로세서에 대한 정보를 담고 있는 객체
    // 현재 스트림 처리 중인 토폴로지의 토픽 정보, 애플리케이션 ID 조회
    // schedule(), forward(), commit() 등의 프로세싱 처리에 필요한 메서드 지원
    private ProcessorContext context;

    // 스트림 프로세서의 생성자로 필요한 리소스를 선언한다.
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    // 프로세싱 로직이 들어간다.
    // 하나의 레코드의 키, 값을 파라미터로 받는다.
    // forward() : 다음 프로세서로 넘어간다.
    // commit() : 처리 완료 후 호출하여 데이터가 처리되었음을 명시적으로 선언한다.
    @Override
    public void process(String key, String value) {
        if (value.length() > 5) {
            context.forward(key, value);
        }
        context.commit();
    }

    // 프로세서가 종료되기 전에 호출되는 메소드.
    // 리소스 해제 로직을 구현한다.
    @Override
    public void close() {}
}
