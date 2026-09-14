"""
Симуляция кластера Raft в одном процессе.
Используется для тестирования.
"""
import time
from typing import Dict, Optional, List, Any
from .node import RaftNode
from .types import RaftConfig, NodeState
from .types import AppendEntriesRequest, RequestVoteRequest


class RaftCluster:
    """Симуляция кластера Raft в памяти"""
    
    def __init__(self, node_ids: List[str], config_factory=None):
        """
        Args:
            node_ids: список ID узлов
            config_factory: функция для создания конфигурации для каждого узла
        """
        self.node_ids = node_ids
        self.servers: Dict[str, RaftNode] = {}
        self.network_delay = 0  # задержка в секундах для симуляции сети
        self.simulated_time_ms = 0  # симулированное время
        self.partitions: Dict[str, set] = {}  # сетевые разделения
        
        # Создать сервера
        for node_id in node_ids:
            if config_factory:
                config = config_factory(node_id)
            else:
                config = RaftConfig(node_id=node_id)
            
            server = RaftNode(config, node_ids)
            self.servers[node_id] = server
        
        # История RPC вызовов для отладки
        self.rpc_history: List[Dict[str, Any]] = []
    
    def get_leader(self) -> Optional[str]:
        """Получить ID текущего лидера (если есть)"""
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                return node_id
        return None
    
    def get_leaders(self) -> List[str]:
        """Получить всех лидеров (при сетевом разделении может быть несколько)"""
        leaders = []
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                leaders.append(node_id)
        return leaders
    
    def tick_ms(self, duration_ms: float = 100) -> None:
        """
        Выполнить шаг симуляции.
        
        Args:
            duration_ms: длительность шага в миллисекундах
        """
        self.simulated_time_ms += duration_ms
        
        for server in self.servers.values():
            server.tick(self.simulated_time_ms)
        
        # Обработать RPC между серверами
        self._process_rpcs()
    
    def _can_reach(self, from_id: str, to_id: str) -> bool:
        """Может ли from_id связаться с to_id"""
        if from_id == to_id:
            return True
        
        # Проверить разделения
        if from_id in self.partitions:
            if to_id in self.partitions[from_id]:
                return False
        
        return True
    
    def _process_rpcs(self) -> None:
        """Обработать RPC вызовы"""
        # Candidate отправляет RequestVote
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.CANDIDATE:
                for peer_id in server.peer_ids:
                    if not self._can_reach(node_id, peer_id):
                        continue
                    
                    peer = self.servers[peer_id]

                    # TODO: создать экземпляр RequestVoteRequest с правильными полями
                    req = None

                    resp = peer.request_vote(req)
                    
                    # Обновить информацию candidate
                    if resp.term > server.state.current_term:
                        # TODO: обновить server.state с помощью методов set_term и become_follower
                        pass
                    elif resp.vote_granted:
                        # TODO: обновить server.state.votes_received, записывая получение голоса
                        pass
                
                # Проверить, получил ли большинство голосов
                if server.state.state == NodeState.CANDIDATE:  # еще не стал follower
                    # TODO: посчитать голоса server.state.votes_received
                    votes_count = None
                    # TODO: посчитать, сколько голосов нужно для избрания себя лидером (см. self.node_ids)
                    needed = None

                    if votes_count >= needed:
                        # TODO: обновить server.state, вызвав become_leader и _notify_state_change
                        # использовать server.peer_ids для передачи в become_leader
                        pass
        
        # Leader отправляет AppendEntries
        for node_id, server in self.servers.items():
            if server.state.state == NodeState.LEADER:
                for peer_id in server.peer_ids:

                    # TODO: если сеть разделена между node_id и peer_id, пропустить отправку AppendEntries этому peer_id
                    # используйте self._can_reach(node_id, peer_id) для проверки связи между узлами
                    
                    peer = self.servers[peer_id]
                    prev_index = server.state.next_index.get(peer_id, 0) - 1
                    prev_term = server.state.log[prev_index].term if prev_index >= 0 and prev_index < len(server.state.log) else 0
                    
                    entries = server.state.get_entries_from(prev_index + 1)
                    
                    # TODO: создать экземпляр AppendEntriesRequest с правильными полями
                    # используйте server.state (current_term, commit_index), node_id, prev_index, prev_term, entries
                    req = None
                    
                    resp = peer.append_entries(req)
                    
                    # Обновить leader state
                    if resp.term > server.state.current_term:
                        # TODO: обновить server.state с помощью методов set_term и become_follower
                        pass
                    elif resp.success:
                        # TODO: обновить server.state.match_index и server.state.next_index для этого peer_id
                        # match_index должен быть индексом последней записи, которая точно есть на peer (prev_index + len(entries))
                        # next_index должен быть на единицу больше match_index
                        pass
                    else:
                        # TODO: если неудача, это означает, что у peer нет некоторых записей, которые есть у лидера. 
                        # Уменьшить next_index для этого peer_id и попробовать снова в следующем тике:
                        # т.е. для peer_id в server.state.next_index установить значение max(0, resp.last_log_index + 1) 
                        # (resp.last_log_index - это индекс последней записи, которая точно есть на peer)
                        pass
                    
                    # Попробовать продвинуть commit_index
                    self._try_advance_commit_index(server, node_id)
    
    def _try_advance_commit_index(self, leader: RaftNode, leader_id: str) -> None:
        """Попробовать продвинуть commit_index лидера"""
        # Проверить каждую запись, начиная со следующей после commit_index
        for index in range(leader.state.commit_index + 1, len(leader.state.log)):
            # TODO: записи могут быть закомичены только если они из текущего term лидера
            # см. leader.state.log[index].term и leader.state.current_term
            # если запись не из лидера текущего term, пропустить ее 

            # TODO: подсчитать количество узлов (включая лидера) с этой записью
            # используйте leader.peer_ids, чтобы проверить leader.state.match_index
            # проверьте для каждого peer_id, что его match_index >= index, и если да, учтите этот peer_id в подсчете
            count = None

            # TODO: посчитать, сколько голосов нужно для кворума (см. self.node_ids)
            needed = None

            # TODO: если кворум имеет эту запись (используйте count и needed), закоммитить
            # обновить leader.state.commit_index текущим индексом (index)
            # вызвать callback leader._apply_committed_entries()
    
    def partition(self, nodes1: List[str], nodes2: List[str]) -> None:
        """
        Создать сетевое разделение между двумя группами узлов.
        
        Args:
            nodes1: первая группа
            nodes2: вторая группа
        """
        # Очистить старые разделения
        self.partitions.clear()
        
        # Добавить новые разделения
        for node_id in nodes1:
            self.partitions[node_id] = set(nodes2)
        for node_id in nodes2:
            self.partitions[node_id] = set(nodes1)
    
    def heal_partition(self) -> None:
        """Исцелить все сетевые разделения"""
        self.partitions.clear()
    
    def wait_for_leader_ms(self, timeout_ms: float = 10000.0) -> Optional[str]:
        """
        Дождаться выборов лидера.
        
        Args:
            timeout_ms: максимальное время ожидания в миллисекундах
            
        Returns:
            ID лидера или None если timeout
        """
        start_time = self.simulated_time_ms
        while self.simulated_time_ms - start_time < timeout_ms:
            leader = self.get_leader()
            if leader:
                return leader
            self.tick_ms()
        return None
    
    def wait_for_replication_ms(self, timeout_ms: float = 10000.0) -> bool:
        """
        Дождаться пока все узлы синхронизируют логи и применят entries.
        
        Returns:
            True если успешно, False если timeout_ms
        """
        start_time = self.simulated_time_ms
        while self.simulated_time_ms - start_time < timeout_ms:
            # Проверить, все ли узлы имеют одинаковые логи И применили entries
            if self._all_logs_match() and self._all_entries_applied():
                return True
            self.tick_ms()
        return False
    
    def _all_entries_applied(self) -> bool:
        """Проверить, применены ли все entries на всех узлах"""
        if not self.servers:
            return True
        
        # Получить максимальный commit_index
        max_commit_index = max(
            (srv.state.commit_index for srv in self.servers.values()),
            default=-1
        )
        
        # Все узлы должны иметь log_len > max_commit_index (т.е. иметь все entries)
        # и last_applied должен быть >= max_commit_index
        for server in self.servers.values():
            if len(server.state.log) <= max_commit_index:
                return False
            if server.state.last_applied < max_commit_index:
                return False
        
        return True
    
    def _all_logs_match(self) -> bool:
        """Проверить, совпадают ли логи всех узлов"""
        if not self.servers:
            return True
        
        first_log = list(self.servers.values())[0].state.log
        for server in self.servers.values():
            if len(server.state.log) != len(first_log):
                return False
            for i, entry in enumerate(server.state.log):
                if first_log[i].term != entry.term or first_log[i].command != entry.command:
                    return False
        return True
    
    def get_cluster_state(self) -> Dict[str, Any]:
        """Получить состояние всего кластера"""
        return {
            "simulated_time": self.simulated_time_ms,
            "leader": self.get_leader(),
            "servers": {
                node_id: server.get_state()
                for node_id, server in self.servers.items()
            }
        }
