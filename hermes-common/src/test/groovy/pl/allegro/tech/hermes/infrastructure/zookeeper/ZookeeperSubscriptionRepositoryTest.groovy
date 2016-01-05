package pl.allegro.tech.hermes.infrastructure.zookeeper

import pl.allegro.tech.hermes.api.*
import pl.allegro.tech.hermes.api.helpers.Patch
import pl.allegro.tech.hermes.domain.subscription.SubscriptionNotExistsException
import pl.allegro.tech.hermes.domain.topic.TopicNotExistsException
import pl.allegro.tech.hermes.test.IntegrationTest

import static pl.allegro.tech.hermes.api.Subscription.Builder.subscription

class ZookeeperSubscriptionRepositoryTest extends IntegrationTest {

    private static final String GROUP = "subscriptionRepositoryGroup"

    private static final TopicName TOPIC = new TopicName(GROUP, 'topic') 
    
    private ZookeeperSubscriptionRepository repository = new ZookeeperSubscriptionRepository(zookeeper(), mapper, paths, topicRepository)

    void setup() {
        if(!groupRepository.groupExists(GROUP)) {
            groupRepository.createGroup(Group.from(GROUP))
            topicRepository.createTopic(Topic.Builder.topic().withName(TOPIC).build())
        }
    }
    
    def "should create subscription"() {
        given:
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('create').build())
        wait.untilSubscriptionCreated(TOPIC, 'create')
        
        expect:
        repository.listSubscriptionNames(TOPIC).contains('create')
    }
    
    def "should throw exception when trying to add subscription to unknonw topic"() {
        when:
        repository.createSubscription(subscription().withTopicName(GROUP, 'unknown').withName('unknown').build())
        
        then:
        thrown(TopicNotExistsException)
    }
    
    def "should return names of all defined subscriptions"() {
        given:
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('listNames1').build())
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('listNames2').build())
        wait.untilSubscriptionCreated(TOPIC, 'listNames1')
        wait.untilSubscriptionCreated(TOPIC, 'listNames2')
        
        expect:
        repository.listSubscriptionNames(TOPIC).containsAll('listNames1', 'listNames2')
    }
    
    def "should return true when subscription exists"() {
        given:
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('exists').build())
        wait.untilSubscriptionCreated(TOPIC, 'exists')
        
        expect:
        repository.subscriptionExists(TOPIC, 'exists')
    }
    
    def "should return false when subscription does no exist"() {
        expect:
        !repository.subscriptionExists(TOPIC, 'unknown')
    }
    
    def "should throw exception when subscription does not exist"() {
        when:
        repository.ensureSubscriptionExists(TOPIC, 'unknown')
        
        then:
        thrown(SubscriptionNotExistsException)
    }
    
    def "should return subscription details"() {
        given:
        repository.createSubscription(subscription()
                .withTopicName(TOPIC)
                .withName('details')
                .withDescription('my description')
                .withEndpoint(EndpointAddress.of('hello'))
                .build())
        wait.untilSubscriptionCreated(TOPIC, 'details')
        
        when:
        Subscription subscription = repository.getSubscriptionDetails(TOPIC, 'details')
        
        then:
        subscription.description == 'my description'
        subscription.endpoint == EndpointAddress.of('hello')
    }
    
    def "should throw exception when trying to return details of unknown subscription"() {
        when:
        repository.getSubscriptionDetails(TOPIC, 'unknown')
        
        then:
        thrown(SubscriptionNotExistsException)
    }
    
    def "should return details of topics subscriptions"() {
        given:
        Subscription subscription1 = subscription().withTopicName(TOPIC).withName('list1').build()
        Subscription subscription2 = subscription().withTopicName(TOPIC).withName('list2').build()
        repository.createSubscription(subscription1)
        repository.createSubscription(subscription2)
        wait.untilSubscriptionCreated(TOPIC, 'list1')
        wait.untilSubscriptionCreated(TOPIC, 'list2')
        
        expect:
        repository.listSubscriptions(TOPIC).containsAll(subscription1, subscription2)
    }
    
    def "should remove subscription"() {
        given:
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('remove').build())
        wait.untilSubscriptionCreated(TOPIC, 'remove')

        when:
        repository.removeSubscription(TOPIC, 'remove')
        
        then:
        !repository.subscriptionExists(TOPIC, 'remove')
    }
    
    def "should change subscription state"() {
        given:
        repository.createSubscription(subscription().withTopicName(TOPIC).withName('state').build())
        wait.untilSubscriptionCreated(TOPIC, 'state')
        
        when:
        repository.updateSubscriptionState(TOPIC, 'state', Subscription.State.SUSPENDED)
        
        then:
        repository.getSubscriptionDetails(TOPIC, 'state').state == Subscription.State.SUSPENDED
    }

    def "should change subscription endpoint"() {
        given:
        def retrieved = subscription()
                .withTopicName(TOPIC)
                .withName('endpoint')
                .withEndpoint(EndpointAddress.of("http://localhost:8080/v1"))
                .build()

        repository.createSubscription(retrieved)
        wait.untilSubscriptionCreated(TOPIC, 'endpoint')

        when:
        def updated = Patch.apply(retrieved, subscription().withEndpoint(EndpointAddress.of("http://localhost:8080/v2")).build());

        repository.updateSubscription(updated)

        then:
        repository.getSubscriptionDetails(TOPIC, 'endpoint').endpoint == EndpointAddress.of("http://localhost:8080/v2")
    }
}
